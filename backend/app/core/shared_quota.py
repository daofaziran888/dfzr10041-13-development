"""跨来源共享额度采集：每轮从各来源日志批次共取三个名额。

规则摘要
--------
- 每轮总名额 ``total_quota=3``；每个来源各有一个基础名额，剩下的名额是共享名额。
- 某来源当前批次没有合格原行时，它的基础名额可以借给另一来源；借用方多出的
  候选逐条结算。
- 空行、格式错行、编码错行不占采集名额，但都会登记来源与行号，并且**推进可恢复
  的原始字节位置**。位置按来源真实编码、包含行尾换行符的字节范围计算，下一轮
  从第一条未确认的原始字节继续，绝不会从半个字符中间开读。
- 采集确认结果驱动“跨来源采集入账清单”；分类后的有效事件批次写入库存，
  来源进度（游标）与库存里的实际原行一一对应。
- 同一来源同一字节范围的同一原行重复提交，只记一次名额、只入一次库存。
- 迟到的来源基础名额与在途借用冲突时，由 ``policy`` 决定：

  * ``BorrowPolicy.BORROW_ON_CLAIM``：借用一经领用立即生效。冲突轮先写借用方，
    迟到来源顺延到下一轮。
  * ``BorrowPolicy.RESERVE_UNTIL_SUBMIT``：提交前仍保留来源的保底额度。借用尚在
    途时，冲突轮收回借用、先写迟到来源，被收回的候选顺延到下一轮。

- 入库成功但回传确认失败时，认领状态停在“已入库待确认”，同一轮次可只重试确认，
  不会重复入库、也不会让来源进度越过实际入账的原行。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from app.core.models import (
    EventRecord,
    IngestJob,
    LogEntry,
    LogSourceConfig,
    ParseResultStatus,
    ParseRule,
)
from app.core.pipeline import PRESET_PARSE_RULES, IngestPipeline


class SlotKind(str, Enum):
    """名额类型。"""

    BASE = "base"          # 来源自己的基础名额
    SHARED = "shared"      # 每轮共用的弹性名额
    BORROWED = "borrowed"  # 从暂时无合格原行的来源借来的基础名额


class BorrowPolicy(str, Enum):
    """跨来源借用冲突的两种结算口径。"""

    BORROW_ON_CLAIM = "borrow_on_claim"
    RESERVE_UNTIL_SUBMIT = "reserve_until_submit"


class ClaimStatus(str, Enum):
    """认领及入账状态。"""

    CLAIMED = "claimed"        # 已领取名额，等待本轮结算
    INGESTED = "ingested"      # 已写入库存，等待回传确认
    CONFIRMED = "confirmed"    # 回传确认完成
    PREEMPTED = "preempted"    # 借用被撤回，顺延下一轮
    DEFERRED = "deferred"      # 名额不足，顺延下一轮
    DUPLICATE = "duplicate"    # 重复提交的原行，只记一次名额


class EntryState(str, Enum):
    """来源内单行的处理状态，用于计算可恢复游标。"""

    PENDING = "pending"    # 合格原行，尚未确认
    ERROR = "error"        # 错行已登记，不占名额
    CLAIMED = "claimed"    # 本轮已认领，尚未结算
    INGESTED = "ingested"  # 已入库存
    CONSUMED = "consumed"  # 空行/错行已越过


@dataclass
class ErrorLine:
    """格式错行登记：来源、行号、字节范围只登记一次。"""

    source_key: str
    source_id: str
    line_number: int
    byte_start: int
    byte_end: int
    status: str
    raw_line: str
    entry_key: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_key": self.source_key,
            "source_id": self.source_id,
            "line_number": self.line_number,
            "byte_start": self.byte_start,
            "byte_end": self.byte_end,
            "status": self.status,
            "raw_line": self.raw_line,
            "entry_key": self.entry_key,
        }


@dataclass
class QuotaClaim:
    """一条跨来源采集认领，同时作为入账清单的一行。"""

    claim_id: str
    round_index: int
    source_key: str
    entry: LogEntry
    slot_kind: SlotKind
    donor_key: str | None = None
    status: ClaimStatus = ClaimStatus.CLAIMED
    record_ids: list[str] = field(default_factory=list)
    event_type_ids: list[str] = field(default_factory=list)
    confirmation_error: str | None = None
    conflict_with: str | None = None
    sequence: int = 0

    # ---- 方便断言的只读视图 ----
    @property
    def entry_key(self) -> str:
        return self.entry.entry_key

    @property
    def source_id(self) -> str:
        return self.entry.source_id

    @property
    def line_number(self) -> int:
        return self.entry.line_number

    @property
    def byte_start(self) -> int:
        return self.entry.byte_start

    @property
    def byte_end(self) -> int:
        return self.entry.byte_end

    @property
    def raw_line(self) -> str:
        return self.entry.raw_line

    @property
    def written(self) -> bool:
        return self.status in (ClaimStatus.INGESTED, ClaimStatus.CONFIRMED)

    def as_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "round": self.round_index,
            "source_key": self.source_key,
            "source_id": self.source_id,
            "line_number": self.line_number,
            "byte_start": self.byte_start,
            "byte_end": self.byte_end,
            "entry_key": self.entry_key,
            "slot_kind": self.slot_kind.value,
            "donor_key": self.donor_key,
            "status": self.status.value,
            "record_ids": list(self.record_ids),
            "event_type_ids": list(self.event_type_ids),
            "confirmation_error": self.confirmation_error,
            "raw_line": self.raw_line,
        }


@dataclass
class RoundResult:
    """一轮跨来源结算的结果。"""

    round_index: int
    policy: BorrowPolicy
    written: list[QuotaClaim] = field(default_factory=list)
    deferred: list[QuotaClaim] = field(default_factory=list)
    preempted: list[QuotaClaim] = field(default_factory=list)
    errors: list[ErrorLine] = field(default_factory=list)
    confirmation_failures: list[QuotaClaim] = field(default_factory=list)
    source_progress: dict[str, int] = field(default_factory=dict)

    @property
    def written_lines(self) -> list[str]:
        return [claim.raw_line for claim in self.written]

    @property
    def written_by_source(self) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for claim in self.written:
            grouped.setdefault(claim.source_key, []).append(claim.raw_line)
        return grouped

    @property
    def deferred_by_source(self) -> dict[str, list[QuotaClaim]]:
        grouped: dict[str, list[QuotaClaim]] = {}
        for claim in self.deferred:
            grouped.setdefault(claim.source_key, []).append(claim)
        return grouped

    @property
    def error_lines(self) -> list[tuple[str, int]]:
        return [(item.source_key, item.line_number) for item in self.errors]

    @property
    def pending_confirmation(self) -> list[QuotaClaim]:
        return list(self.confirmation_failures)


@dataclass
class _SourceState:
    """单个来源的字节数据、扫描位置和行状态。"""

    key: str
    source: LogSourceConfig
    job: IngestJob
    order: int
    encoding: str
    rule: ParseRule | None = None
    data: bytearray = field(default_factory=bytearray)
    scan_offset: int = 0
    finalized: bool = False
    entries: list[LogEntry] = field(default_factory=list)
    entry_states: dict[str, EntryState] = field(default_factory=dict)


ConfirmHook = Callable[[QuotaClaim], Any]


class CrossSourceQuotaCollector:
    """跨来源共享额度采集协调器。

    典型用法::

        collector = CrossSourceQuotaCollector(pipeline, policy=...
                                             .BORROW_ON_CLAIM)
        collector.register_source("gateway", gateway_source, order=0)
        collector.register_source("app", app_source, order=1)
        collector.begin_round()
        collector.feed("app", app_bytes)
        collector.feed("gateway", header_bytes)
        app_claims = [collector.claim("app") for _ in range(2)]
        borrowed = collector.claim("app")       # 借网关暂时空出的基础名额
        collector.feed("gateway", g1_bytes)     # 网关合法日志迟到
        late = collector.claim("gateway")
        result = collector.settle_round()
    """

    ENTRY_ERROR_STATUSES = {
        ParseResultStatus.FORMAT_MISMATCH,
        ParseResultStatus.ENCODING_ERROR,
    }

    def __init__(
        self,
        pipeline: IngestPipeline | None = None,
        *,
        total_quota: int = 3,
        base_quota: int = 1,
        policy: BorrowPolicy | str = BorrowPolicy.RESERVE_UNTIL_SUBMIT,
    ) -> None:
        self.pipeline = pipeline or IngestPipeline()
        self.total_quota = total_quota
        self.base_quota = base_quota
        self.policy = BorrowPolicy(policy)
        self._sources: dict[str, _SourceState] = {}
        self._round_index = 0
        self._round_open = False
        self._round_sequence = 0
        self._round_claims: list[QuotaClaim] = []
        self._round_errors: list[ErrorLine] = []
        self._base_claims: dict[str, QuotaClaim] = {}
        self._loans: dict[str, QuotaClaim] = {}
        self._shared_used = 0
        self._shared_capacity = 0
        self._claims: list[QuotaClaim] = []
        self._claim_by_key: dict[str, QuotaClaim] = {}
        self._seen_keys: set[str] = set()
        self._ingested_keys: set[str] = set()
        self._ingested_content_keys: dict[str, str] = {}
        self._error_ledger: dict[str, ErrorLine] = {}
        self._error_content_keys: dict[str, ErrorLine] = {}
        self._last_result: RoundResult | None = None
        self.duplicate_submissions = 0

    # ------------------------------------------------------------------
    # 来源登记 / 数据投递
    # ------------------------------------------------------------------

    def register_source(
        self,
        key: str,
        source: LogSourceConfig,
        job: IngestJob | None = None,
        *,
        order: int | None = None,
        encoding: str | None = None,
        rule: ParseRule | None = None,
    ) -> _SourceState:
        """登记一个来源及其采集作业。``order`` 决定“来源顺序”。

        未显式给出解析规则时，按来源编号从预置规则里取第一条生效规则，
        保证分类阶段能拿到解析字段。
        """
        if key in self._sources:
            raise ValueError(f"来源 {key} 已登记")
        encoding = encoding or source.encoding or "utf-8"
        if job is None:
            job = IngestJob(
                job_id=f"INGE-{key}",
                source_id=source.source_id,
                encoding=encoding,
            )
        job.encoding = encoding
        if rule is None:
            rule = next(
                (
                    item for item in PRESET_PARSE_RULES
                    if item.source_id == source.source_id and item.state == "生效中"
                ),
                None,
            )
        state = _SourceState(
            key=key,
            source=source,
            job=job,
            order=order if order is not None else len(self._sources),
            encoding=encoding,
            rule=rule,
        )
        self._sources[key] = state
        return state

    def feed(self, source_key: str, data: bytes | str) -> _SourceState:
        """向来源追加新到达的原始字节（按该来源的真实编码编码）。"""
        state = self._require(source_key)
        if isinstance(data, str):
            data = data.encode(state.encoding)
        state.data.extend(data)
        return state

    # 别名：append / push 更容易被调用方接受
    append = feed
    push = feed

    def finalize_source(self, source_key: str) -> list[LogEntry]:
        """标记来源当前文件已结束：最后一条无换行残行也可以被采集。"""
        state = self._require(source_key)
        state.finalized = True
        return self.scan(source_key)

    def scan(self, source_key: str) -> list[LogEntry]:
        """从上次扫描位置继续按字节切行，登记错行，更新行状态。

        返回本次扫描新增的日志条目。未写换行的残行在来源未结束前不会被读出，
        因此下一轮一定从完整的行首开始，不会出现半个中文字符。
        """
        state = self._require(source_key)
        new_entries = self.pipeline.read_source_lines(
            state.source,
            bytes(state.data),
            start=state.scan_offset,
            encoding=state.encoding,
            rule=state.rule,
            include_partial=state.finalized,
        )
        added: list[LogEntry] = []
        for entry in new_entries:
            key = entry.entry_key
            if key in state.entry_states:
                continue
            state.entries.append(entry)
            added.append(entry)
            if entry.parse_status in self.ENTRY_ERROR_STATUSES:
                state.entry_states[key] = EntryState.ERROR
                self._register_error(state, entry)
            elif entry.parse_status == ParseResultStatus.LINE_EMPTY:
                # 空行同样不占名额，只需要知道它的位置；不登记为格式错行。
                state.entry_states[key] = EntryState.ERROR
            elif entry.parse_status == ParseResultStatus.OK:
                state.entry_states[key] = EntryState.PENDING
            else:
                # 字段缺失等无法生成事件的合格格式行：登记后不占名额。
                state.entry_states[key] = EntryState.ERROR
                self._register_error(state, entry)
            if entry.terminated or state.finalized:
                state.scan_offset = entry.byte_end
        return added

    def _register_error(self, state: _SourceState, entry: LogEntry) -> None:
        key = entry.entry_key
        if key in self._error_ledger:
            return
        # 重复提交同一原行：错行也只登记一次，保留第一次的来源与行号。
        content_key = entry.compute_content_key()
        if content_key in self._error_content_keys:
            self._error_ledger[key] = self._error_content_keys[content_key]
            state.entry_states[key] = EntryState.CONSUMED
            return
        record = ErrorLine(
            source_key=state.key,
            source_id=entry.source_id,
            line_number=entry.line_number,
            byte_start=entry.byte_start,
            byte_end=entry.byte_end,
            status=entry.parse_status.value,
            raw_line=entry.raw_line,
            entry_key=key,
        )
        self._error_ledger[key] = record
        self._error_content_keys[content_key] = record
        self._round_errors.append(record)

    # ------------------------------------------------------------------
    # 轮次与名额领取
    # ------------------------------------------------------------------

    def begin_round(self) -> int:
        """开启新一轮：清空本轮名额占用，保留来源扫描位置与已入账状态。"""
        self._round_index += 1
        self._round_open = True
        self._round_sequence = 0
        self._round_claims = []
        self._round_errors = []
        self._base_claims = {}
        self._loans = {}
        self._shared_used = 0
        self._shared_capacity = max(
            self.total_quota - self.base_quota * len(self._sources), 0
        )
        return self._round_index

    next_round = begin_round

    def claim(self, source_key: str, entry: LogEntry | None = None) -> QuotaClaim | None:
        """为来源领取下一格名额。

        没有合格原行时返回 ``None``；重复提交同一原行时返回已有的认领并把
        ``duplicate_submissions`` 加一，绝不会重复占名额。
        """
        state = self._require(source_key)
        if not self._round_open:
            self.begin_round()
        self.scan(source_key)
        if entry is None:
            entry = self._next_candidate(state)
        if entry is None:
            return None
        key = entry.entry_key

        existing = self._claim_by_key.get(key)
        if existing is not None and existing.status in (
            ClaimStatus.CLAIMED,
            ClaimStatus.INGESTED,
            ClaimStatus.CONFIRMED,
        ):
            self.duplicate_submissions += 1
            return existing
        if key in self._seen_keys or key in self._ingested_keys:
            self.duplicate_submissions += 1
            return self._claim_by_key.get(key)
        # 相同原行在新位置再次提交：只记一次名额，直接复用已入账的认领。
        content_key = entry.compute_content_key()
        prior_key = self._ingested_content_keys.get(content_key)
        if prior_key is not None and prior_key != key:
            self.duplicate_submissions += 1
            state.entry_states[key] = EntryState.CONSUMED
            return self._claim_by_key.get(prior_key)
        if existing is not None and existing.round_index == self._round_index:
            # 本轮已经因为它名额不足而顺延，不再重复占用。
            return existing

        slot_kind, donor = self._allocate(state)
        if slot_kind is None:
            if existing is not None:
                # 沿用上一轮的顺延记录，避免入账清单出现重复行。
                return existing
            claim = self._new_claim(state, entry, None, None)
            claim.status = ClaimStatus.DEFERRED
            state.entry_states[key] = EntryState.PENDING
            self._round_claims.append(claim)
            self._claims.append(claim)
            self._claim_by_key[key] = claim
            return claim

        if existing is not None:
            # 上一轮顺延的候选重新领取：复用同一条入账记录。
            claim = existing
            claim.round_index = self._round_index
            claim.slot_kind = slot_kind
            claim.donor_key = donor
            claim.status = ClaimStatus.CLAIMED
            claim.confirmation_error = None
            self._round_sequence += 1
            claim.sequence = self._round_sequence
            claim.claim_id = f"CLAM-{self._round_index:03d}-{self._round_sequence:03d}"
        else:
            claim = self._new_claim(state, entry, slot_kind, donor)
            self._claims.append(claim)

        state.entry_states[key] = EntryState.CLAIMED
        self._round_claims.append(claim)
        self._claim_by_key[key] = claim
        if slot_kind == SlotKind.BASE:
            self._base_claims[state.key] = claim
        elif slot_kind == SlotKind.BORROWED and donor is not None:
            self._loans[donor] = claim
        return claim

    # `claim_next` / `claim_many` 让“按来源批量领取”写法更自然。
    claim_next = claim

    def claim_many(self, source_key: str, count: int) -> list[QuotaClaim | None]:
        return [self.claim(source_key) for _ in range(count)]

    def _allocate(self, state: _SourceState) -> tuple[SlotKind | None, str | None]:
        """按“来源基础 → 共享 → 借用暂停来源”的顺序分配一格名额。

        每个来源先结算自己的基础名额，再消耗共享名额；共享名额用尽后才去借
        当前没有合格原行的来源的基础名额。每一格最多分配给一个认领。

        若本来源自己的基础名额本轮已经被别人借走，而它现在也有了合格原行，
        它仍然按基础名额领取这一行；结算时由借用政策裁决谁写、谁顺延，
        不会让一轮写出超过总名额的行。
        """
        if self.base_quota >= 1 and state.key not in self._base_claims:
            return SlotKind.BASE, None
        if self._shared_used < self._shared_capacity:
            self._shared_used += 1
            return SlotKind.SHARED, None
        donor = self._find_donor(state.key)
        if donor is not None:
            return SlotKind.BORROWED, donor
        return None, None

    def _find_donor(self, borrower_key: str) -> str | None:
        """找一个“暂时没有合格原行”且基础名额空闲的来源。

        已经率先领取了自己基础名额的来源不在此列；本轮还没提交的借用也要避开。
        """
        candidates = sorted(self._sources.values(), key=lambda item: item.order)
        for donor in candidates:
            if donor.key == borrower_key:
                continue
            if donor.key in self._base_claims:
                continue
            if donor.key in self._loans:
                continue
            if self._has_qualified_pending(donor):
                continue
            return donor.key
        return None

    def _has_qualified_pending(self, state: _SourceState) -> bool:
        return any(
            self._entry_state(state, entry) == EntryState.PENDING
            and entry.parse_status == ParseResultStatus.OK
            for entry in state.entries
        )

    def _new_claim(
        self,
        state: _SourceState,
        entry: LogEntry,
        slot_kind: SlotKind | None,
        donor: str | None,
    ) -> QuotaClaim:
        self._round_sequence += 1
        claim = QuotaClaim(
            claim_id=f"CLAM-{self._round_index:03d}-{self._round_sequence:03d}",
            round_index=self._round_index,
            source_key=state.key,
            entry=entry,
            slot_kind=slot_kind or SlotKind.SHARED,
            donor_key=donor,
            sequence=self._round_sequence,
        )
        if slot_kind == SlotKind.BASE:
            self._base_claims[state.key] = claim
        elif slot_kind == SlotKind.BORROWED and donor is not None:
            self._loans[donor] = claim
        return claim

    def _next_candidate(self, state: _SourceState) -> LogEntry | None:
        for entry in state.entries:
            if self._entry_state(state, entry) != EntryState.PENDING:
                continue
            if entry.parse_status != ParseResultStatus.OK:
                continue
            if entry.entry_key in self._seen_keys:
                continue
            if entry.compute_content_key() in self._ingested_content_keys:
                # 同一原行已经入账：不占名额，标记越过。
                state.entry_states[entry.entry_key] = EntryState.CONSUMED
                continue
            return entry
        return None

    @staticmethod
    def _entry_state(state: _SourceState, entry: LogEntry) -> EntryState:
        return state.entry_states.get(entry.entry_key, EntryState.PENDING)

    # ------------------------------------------------------------------
    # 结算：冲突裁决 → 串联 run_ingest_job / classify_events → 入账
    # ------------------------------------------------------------------

    def settle_round(self, confirm_hook: ConfirmHook | None = None) -> RoundResult:
        """结清本轮并落库。

        第一次调用完成冲突裁决、采集、分类、入库与确认；如果确认回传失败，
        认领停在“已入库待确认”。再次调用只重试待确认项，不会重复入库。
        """
        if not self._round_open:
            return self.resume_pending(confirm_hook)

        for key in self._sources:
            self.scan(key)

        deferred: list[QuotaClaim] = []
        preempted: list[QuotaClaim] = []

        self._resolve_loans(deferred, preempted)

        # 全局容量兜底：基础名额优先、按来源顺序，超出部分顺延。
        eligible = [
            claim for claim in self._round_claims
            if claim.status == ClaimStatus.CLAIMED
        ]
        eligible.sort(key=self._claim_priority)
        unadmitted = eligible[self.total_quota:]
        for claim in unadmitted:
            claim.status = ClaimStatus.DEFERRED
            deferred.append(claim)
            self._release_entry(claim)
        admitted = eligible[: self.total_quota]

        written: list[QuotaClaim] = []
        failures: list[QuotaClaim] = []
        self._ingest_claims(admitted, written, failures, confirm_hook)

        self._round_open = False
        result = RoundResult(
            round_index=self._round_index,
            policy=self.policy,
            written=list(written),
            deferred=deferred,
            preempted=preempted,
            errors=list(self._round_errors),
            confirmation_failures=failures,
            source_progress=self.source_progress(),
        )
        self._last_result = result
        return result

    settle = settle_round

    def _resolve_loans(
        self,
        deferred: list[QuotaClaim],
        preempted: list[QuotaClaim],
    ) -> None:
        """裁决“借用”与“迟到的来源基础名额”的冲突。"""
        for donor_key, loan in list(self._loans.items()):
            donor_claim = self._base_claims.get(donor_key)
            if donor_claim is None:
                continue
            if donor_claim.status != ClaimStatus.CLAIMED:
                continue
            if self.policy == BorrowPolicy.BORROW_ON_CLAIM:
                # 借用一经领用立即生效，迟到的来源只能顺延。
                donor_claim.status = ClaimStatus.DEFERRED
                donor_claim.conflict_with = loan.claim_id
                deferred.append(donor_claim)
                self._release_entry(donor_claim)
            elif loan.status in (ClaimStatus.INGESTED, ClaimStatus.CONFIRMED):
                # 借用已经提交，无法无损收回；迟到来源顺延。
                donor_claim.status = ClaimStatus.DEFERRED
                donor_claim.conflict_with = loan.claim_id
                deferred.append(donor_claim)
                self._release_entry(donor_claim)
            else:
                # 提交前仍保留来源保底额度：收回借用，先写迟到来源。
                loan.status = ClaimStatus.PREEMPTED
                loan.conflict_with = donor_claim.claim_id
                preempted.append(loan)
                self._release_entry(loan)

    def _claim_priority(self, claim: QuotaClaim) -> tuple[int, int, int]:
        # 基础名额优先；借用一经领用就占住格子，排在普通共享名额之前，
        # 避免迟到来源的后续共享候选把已经生效的借用挤出本轮。
        slot_rank = {
            SlotKind.BASE: 0,
            SlotKind.BORROWED: 1,
            SlotKind.SHARED: 2,
        }[claim.slot_kind]
        order = self._sources[claim.source_key].order
        return (slot_rank, order, claim.sequence)

    def _release_entry(self, claim: QuotaClaim) -> None:
        """顺延/收回的候选项恢复为下一轮可领取。"""
        state = self._sources[claim.source_key]
        if claim.entry_key in state.entry_states:
            state.entry_states[claim.entry_key] = EntryState.PENDING
        self._seen_keys.discard(claim.entry_key)

    def _ingest_claims(
        self,
        admitted: list[QuotaClaim],
        written: list[QuotaClaim],
        failures: list[QuotaClaim],
        confirm_hook: ConfirmHook | None,
    ) -> None:
        by_source: dict[str, list[QuotaClaim]] = {}
        for claim in admitted:
            if claim.status != ClaimStatus.CLAIMED:
                continue
            by_source.setdefault(claim.source_key, []).append(claim)

        for source_key in sorted(by_source, key=lambda key: self._sources[key].order):
            state = self._sources[source_key]
            claims = sorted(by_source[source_key], key=lambda item: item.byte_start)
            prefix = self._consumed_prefix(state, claims)
            # 串联：run_ingest_job 推进字节游标并统计跳过 → classify_events 分类
            self.pipeline.run_ingest_job(
                prefix, state.job, enforce_batch_size=False
            )
            # 游标已达/越过的错行、空行才算 consumed；认领之后的错行留给下一轮先越过。
            cutoff = state.job.file_cursor
            for entry in state.entries:
                if entry.byte_start >= cutoff:
                    break
                if (
                    entry.parse_status != ParseResultStatus.OK
                    and self._entry_state(state, entry) == EntryState.ERROR
                ):
                    state.entry_states[entry.entry_key] = EntryState.CONSUMED
            qualified = [claim.entry for claim in claims]
            records = self.pipeline.classify_events(qualified)
            self.pipeline.store_events(records)

            records_by_key: dict[str, list[EventRecord]] = {}
            for record in records:
                records_by_key.setdefault(record.entry_key, []).append(record)

            for claim in claims:
                matched = records_by_key.get(claim.entry_key, [])
                claim.record_ids = [record.record_id for record in matched]
                claim.event_type_ids = [
                    record.event_type.type_id for record in matched
                ]
                claim.status = ClaimStatus.INGESTED
                state.entry_states[claim.entry_key] = EntryState.INGESTED
                self._seen_keys.add(claim.entry_key)
                self._ingested_keys.add(claim.entry_key)
                self._ingested_content_keys[
                    claim.entry.compute_content_key()
                ] = claim.entry_key
                written.append(claim)

        # 所有来源（含本轮没有认领的来源）都越过“第一条未确认合格行之前”的
        # 错行/空行，保证 job 游标与来源可恢复位置一致，下一轮不会卡在错误头上。
        for state in self._sources.values():
            prefix = self._leading_consumed(state)
            if prefix:
                self.pipeline.run_ingest_job(
                    prefix, state.job, enforce_batch_size=False
                )
            cutoff = state.job.file_cursor
            for entry in state.entries:
                if entry.byte_start >= cutoff:
                    break
                if (
                    entry.parse_status != ParseResultStatus.OK
                    and self._entry_state(state, entry) == EntryState.ERROR
                ):
                    state.entry_states[entry.entry_key] = EntryState.CONSUMED

        # 回传确认：入库成功但确认失败时保留“已入库待确认”，可同轮次恢复。
        for claim in written:
            self._confirm(claim, failures, confirm_hook)

    def _leading_consumed(self, state: _SourceState) -> list[LogEntry]:
        """第一条未确认的合格行之前的错行/空行前缀。"""
        prefix: list[LogEntry] = []
        for entry in state.entries:
            if entry.parse_status == ParseResultStatus.OK and self._entry_state(
                state, entry
            ) in (EntryState.PENDING, EntryState.CLAIMED):
                break
            prefix.append(entry)
        return prefix

    def _consumed_prefix(
        self, state: _SourceState, claims: list[QuotaClaim]
    ) -> list[LogEntry]:
        """返回本轮已确认要越过的连续前缀（错行 + 认领到的合格行）。

        一旦遇到“未认领的错行”，前缀就停在它前面：先把本轮已认领的行结算，
        下一轮再从这条错行之后继续，保证游标不会越过尚未确认的原始字节。
        """
        claimed_keys = {claim.entry_key for claim in claims}
        cutoff = max(claim.byte_end for claim in claims)
        prefix: list[LogEntry] = []
        for entry in state.entries:
            if entry.byte_start >= cutoff:
                break
            if entry.entry_key in claimed_keys:
                prefix.append(entry)
                continue
            current = self._entry_state(state, entry)
            if current in (
                EntryState.CONSUMED,
                EntryState.INGESTED,
            ):
                prefix.append(entry)
                continue
            if current == EntryState.ERROR:
                # 错行已登记但还没被越过：停在这里，下一轮先越过它再继续。
                break
            # 前面出现未确认的合格行时不能越过它。
            break
        return prefix

    def _confirm(
        self,
        claim: QuotaClaim,
        failures: list[QuotaClaim],
        confirm_hook: ConfirmHook | None,
    ) -> None:
        if claim.status == ClaimStatus.CONFIRMED:
            return
        try:
            if confirm_hook is not None:
                confirm_hook(claim)
        except Exception as exc:  # 确认回传失败：保留入库结果，允许断点恢复
            claim.confirmation_error = str(exc)
            report = claim.as_dict()
            report["confirmation_error"] = str(exc)
            if claim not in failures:
                failures.append(claim)
            return
        claim.status = ClaimStatus.CONFIRMED
        claim.confirmation_error = None

    def retry_confirmation(
        self, claim: QuotaClaim | str, confirm_hook: ConfirmHook | None = None
    ) -> QuotaClaim | None:
        """只重试回传确认，不重复采集/入库。"""
        if isinstance(claim, str):
            claim = next(
                (item for item in self._claims if item.claim_id == claim), None
            )
            if claim is None:
                return None
        if claim.status == ClaimStatus.CONFIRMED:
            return claim
        if claim.status != ClaimStatus.INGESTED:
            return None
        failures: list[QuotaClaim] = []
        self._confirm(claim, failures, confirm_hook)
        return claim

    confirm_claim = retry_confirmation

    def resume_pending(self, confirm_hook: ConfirmHook | None = None) -> RoundResult:
        """同一轮次断点恢复：重试所有“已入库待确认”的认领。"""
        failures: list[QuotaClaim] = []
        pending = [
            claim for claim in self._claims
            if claim.status == ClaimStatus.INGESTED
        ]
        for claim in pending:
            if confirm_hook is None:
                # 没有提供回传实现时视为恢复成功。
                claim.status = ClaimStatus.CONFIRMED
                claim.confirmation_error = None
            else:
                self._confirm(claim, failures, confirm_hook)
        result = self._last_result or RoundResult(
            round_index=self._round_index, policy=self.policy
        )
        result.confirmation_failures = failures
        result.source_progress = self.source_progress()
        # 只保留本轮仍在库存中、已确认的写入项。
        result.written = [claim for claim in result.written if claim.written]
        result.deferred = list(result.deferred)
        self._last_result = result
        return result

    recover_round = resume_pending

    # ------------------------------------------------------------------
    # 进度、清单与对账
    # ------------------------------------------------------------------

    def source_cursor(self, source_key: str) -> int:
        """来源的可恢复位置：第一条未确认的合格原行的字节起点。

        错行/空行只登记、不占名额，也不会把游标卡住——它们由 ``_leading_consumed``
        在下一轮开头按真实字节越过。因此游标永远停在一个合格原行的行首，
        不会落到某个多字节字符或半行中间。没有待确认的合格原行时停在扫描位置。
        """
        state = self._require(source_key)
        for entry in state.entries:
            if entry.parse_status != ParseResultStatus.OK:
                continue
            if self._entry_state(state, entry) in (
                EntryState.PENDING,
                EntryState.CLAIMED,
            ):
                return entry.byte_start
        return state.scan_offset

    def source_progress(self) -> dict[str, int]:
        return {key: self.source_cursor(key) for key in self._sources}

    def ledger(self, *, confirmed_only: bool = False) -> list[dict[str, Any]]:
        """跨来源采集入账清单（确认结果驱动）。"""
        rows = []
        for claim in self._claims:
            if confirmed_only and claim.status != ClaimStatus.CONFIRMED:
                continue
            rows.append(claim.as_dict())
        return rows

    def confirmed_claims(self) -> list[QuotaClaim]:
        return [claim for claim in self._claims if claim.status == ClaimStatus.CONFIRMED]

    def ingested_claims(self) -> list[QuotaClaim]:
        return [
            claim for claim in self._claims
            if claim.status in (ClaimStatus.INGESTED, ClaimStatus.CONFIRMED)
        ]

    def error_ledger(self) -> list[dict[str, Any]]:
        unique: dict[int, ErrorLine] = {
            id(record): record for record in self._error_content_keys.values()
        }
        return [record.as_dict() for record in unique.values()]

    def errors_for(self, source_key: str) -> list[ErrorLine]:
        return [
            record for record in self._error_content_keys.values()
            if record.source_key == source_key
        ]

    def inventory(self, source_key: str | None = None) -> list[EventRecord]:
        """库存里的实际事件记录。"""
        records: list[EventRecord] = []
        for items in self.pipeline.get_event_store().values():
            records.extend(items)
        if source_key is None:
            return records
        state = self._require(source_key)
        keys = {
            entry.entry_key
            for entry in state.entries
            if self._entry_state(state, entry) == EntryState.INGESTED
        }
        return [record for record in records if record.entry_key in keys]

    def inventory_lines(self, source_key: str | None = None) -> list[str]:
        return [record.original_content for record in self.inventory(source_key)]

    def reconcile(self, source_key: str | None = None) -> dict[str, Any]:
        """核对来源进度与库存原行：已确认的原始行必须与库存逐条对得上。"""
        keys = [source_key] if source_key else list(self._sources)
        report: dict[str, Any] = {"ok": True, "sources": {}}
        for key in keys:
            state = self._sources[key]
            ingested = [
                entry.entry_key
                for entry in state.entries
                if self._entry_state(state, entry) == EntryState.INGESTED
                and entry.parse_status == ParseResultStatus.OK
            ]
            stored = {record.entry_key for record in self.inventory(key)}
            missing = [item for item in ingested if item not in stored]
            extra = [item for item in stored if item not in set(ingested)]
            cursor = self.source_cursor(key)
            job_cursor = state.job.file_cursor
            source_report = {
                "progress": cursor,
                "job_cursor": job_cursor,
                "ingested_lines": len(ingested),
                "inventory_lines": len(stored),
                "missing": missing,
                "extra": extra,
                "progress_matches_inventory": (
                    not missing and not extra and cursor == job_cursor
                ),
            }
            report["sources"][key] = source_report
            if missing or extra or cursor != job_cursor:
                report["ok"] = False
        return report

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _require(self, source_key: str) -> _SourceState:
        state = self._sources.get(source_key)
        if state is None:
            raise KeyError(f"来源 {source_key} 未登记")
        return state

    def get_claim(self, entry_key: str) -> QuotaClaim | None:
        return self._claim_by_key.get(entry_key)

    def claims_for_source(self, source_key: str) -> list[QuotaClaim]:
        return [claim for claim in self._claims if claim.source_key == source_key]

    @property
    def round_index(self) -> int:
        return self._round_index

    @property
    def round_open(self) -> bool:
        return self._round_open


def make_shared_quota_collector(
    pipeline: IngestPipeline | None = None,
    *,
    total_quota: int = 3,
    base_quota: int = 1,
    policy: BorrowPolicy | str = BorrowPolicy.RESERVE_UNTIL_SUBMIT,
) -> CrossSourceQuotaCollector:
    """工厂函数：创建跨来源共享额度采集协调器。"""
    return CrossSourceQuotaCollector(
        pipeline,
        total_quota=total_quota,
        base_quota=base_quota,
        policy=policy,
    )
