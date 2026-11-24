"""测试：跨来源共享额度采集 shared_quota。

覆盖数据流：
- 每轮总名额 3：来源基础名额 + 共享名额 + 借用名额
- 某来源暂无合格原行时，基础名额可借给另一来源
- 借用与迟到来源基础名额的两种结算口径
- 空行/错行/中文与含行尾字节范围的可恢复游标
- 重复提交原行只记一次名额
- 入库成功但回传确认失败的同一轮次断点恢复
- 来源进度与库存原行对账
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.pipeline import PRESET_LOG_SOURCES, make_pipeline
from app.core.shared_quota import (
    BorrowPolicy,
    ClaimStatus,
    CrossSourceQuotaCollector,
    EntryState,
    SlotKind,
    make_shared_quota_collector,
)

GATEWAY = PRESET_LOG_SOURCES["access-web"]      # LOG_-0001
APP = PRESET_LOG_SOURCES["app-backend"]         # LOG_-0002


def app_line(n: int, level: str = "INFO") -> str:
    return (
        f"2026-11-15 14:30:0{n}.123 [http-nio-8080-exec-{n}] {level}  "
        f"com.example.App - 采集事件 P{n} 中文原行"
    )


def access_line(n: int) -> str:
    return (
        f"10.0.0.{n} - - [15/Nov/2026:14:30:0{n} +0800] "
        f'"GET /api/g{n} HTTP/1.1" 200 {n} "-" "curl/8"'
    )


BAD_HEADER = "### 已判格式不匹配的头部 ###"


def build_collector(policy: BorrowPolicy) -> CrossSourceQuotaCollector:
    collector = CrossSourceQuotaCollector(make_pipeline(), policy=policy)
    collector.register_source("gateway", GATEWAY, order=0)
    collector.register_source("app", APP, order=1)
    return collector


class TestScenarioBothPolicies:
    """值班员场景：应用已领 P1/P2 并借第三格，网关 G1 迟到。"""

    def _round_one(self, policy: BorrowPolicy) -> CrossSourceQuotaCollector:
        collector = build_collector(policy)
        # 网关先读到已判格式不匹配的头部，错行登记且不占名额。
        collector.feed("gateway", BAD_HEADER + "\n")
        collector.feed("app", "".join(line + "\n" for line in
                                     (app_line(1), app_line(2), app_line(3))).encode())
        collector.begin_round()
        p1 = collector.claim("app")
        p2 = collector.claim("app")
        borrowed = collector.claim("app")
        assert (p1.slot_kind, p2.slot_kind) == (SlotKind.BASE, SlotKind.SHARED)
        assert borrowed.slot_kind == SlotKind.BORROWED
        assert borrowed.donor_key == "gateway"
        # 网关合法日志 G1 迟到。
        collector.feed("gateway", (access_line(1) + "\n").encode())
        late = collector.claim("gateway")
        assert late is not None and late.slot_kind == SlotKind.BASE
        return collector

    def test_borrow_on_claim_writes_p1_p2_p3(self):
        collector = self._round_one(BorrowPolicy.BORROW_ON_CLAIM)
        result = collector.settle_round()
        assert result.written_lines == [app_line(1), app_line(2), app_line(3)]
        # 迟到的 G1 顺延到下一轮。
        assert [claim.raw_line for claim in result.deferred] == [access_line(1)]
        # 错行只登记一次，来源和行号都在。
        assert result.error_lines == [("gateway", 1)]

        collector.begin_round()
        collector.claim("gateway")
        second = collector.settle_round()
        assert second.written_lines == [access_line(1)]
        assert collector.reconcile()["ok"] is True

    def test_reserve_until_submit_writes_p1_p2_g1(self):
        collector = self._round_one(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        result = collector.settle_round()
        assert result.written_lines == [access_line(1), app_line(1), app_line(2)]
        # 借用在提交前被收回，P3 顺延到下一轮。
        assert [claim.raw_line for claim in result.preempted] == [app_line(3)]
        assert result.deferred == []
        assert result.error_lines == [("gateway", 1)]

        collector.begin_round()
        collector.claim("app")
        second = collector.settle_round()
        assert second.written_lines == [app_line(3)]
        assert collector.reconcile()["ok"] is True

    def test_late_line_exists_only_once_in_inventory(self):
        collector = self._round_one(BorrowPolicy.BORROW_ON_CLAIM)
        collector.settle_round()
        collector.begin_round()
        collector.claim("gateway")
        collector.settle_round()
        lines = collector.inventory_lines()
        assert lines.count(access_line(1)) == 1
        assert lines.count(app_line(3)) == 1

    def test_error_header_registered_only_once(self):
        collector = self._round_one(BorrowPolicy.BORROW_ON_CLAIM)
        collector.settle_round()
        collector.begin_round()
        collector.claim("gateway")
        collector.settle_round()
        errors = collector.errors_for("gateway")
        assert len(errors) == 1
        assert errors[0].line_number == 1
        assert errors[0].raw_line == BAD_HEADER


class TestQuotaAllocation:
    """名额分配：总 3 格，基础 + 共享 + 借用。"""

    def test_total_quota_is_three(self):
        collector = build_collector(BorrowPolicy.BORROW_ON_CLAIM)
        collector.feed("app", "".join(app_line(n) + "\n" for n in range(1, 6)).encode())
        collector.begin_round()
        claims = [collector.claim("app") for _ in range(5)]
        assert all(claim is not None for claim in claims)
        result = collector.settle_round()
        assert len(result.written) == 3
        # 其余候选没有可用名额，停在待领取状态（DEFERRED）。
        assert sum(1 for claim in claims if claim.status == ClaimStatus.DEFERRED) == 2

    def test_base_quota_per_source(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("gateway", (access_line(1) + "\n" + access_line(2) + "\n").encode())
        collector.feed("app", (app_line(1) + "\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        gateway_first = collector.claim("gateway")
        app_first = collector.claim("app")
        assert gateway_first.slot_kind == SlotKind.BASE
        assert app_first.slot_kind == SlotKind.BASE
        third = collector.claim("gateway")
        assert third.slot_kind == SlotKind.SHARED
        result = collector.settle_round()
        assert len(result.written) == 3

    def test_donor_must_have_no_qualified_line(self):
        collector = build_collector(BorrowPolicy.BORROW_ON_CLAIM)
        collector.feed("gateway", (access_line(1) + "\n").encode())
        collector.feed("app", "".join(app_line(n) + "\n" for n in range(1, 5)).encode())
        collector.begin_round()
        assert collector.claim("gateway").slot_kind == SlotKind.BASE
        assert collector.claim("app").slot_kind == SlotKind.BASE
        borrowed = collector.claim("app")
        # 网关还有合格原行，不能把它当停采来源借名额。
        assert borrowed.slot_kind == SlotKind.SHARED
        fourth = collector.claim("app")
        assert fourth.status == ClaimStatus.DEFERRED

    def test_no_donor_when_all_sources_have_lines(self):
        collector = build_collector(BorrowPolicy.BORROW_ON_CLAIM)
        collector.feed("gateway", (access_line(1) + "\n" + access_line(2) + "\n").encode())
        collector.feed("app", (app_line(1) + "\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        collector.claim("gateway")
        collector.claim("app")
        collector.claim("gateway")
        overflow = collector.claim("app")
        assert overflow.status == ClaimStatus.DEFERRED


class TestByteAccurateCursor:
    """游标按来源真实编码、含行尾的字节范围推进。"""

    def test_error_and_empty_lines_advance_cursor(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        payload = "\n".join([BAD_HEADER, "", app_line(1), app_line(2)]).encode() + b"\n"
        collector.feed("app", payload)
        collector.begin_round()
        collector.claim("app")
        result = collector.settle_round()
        assert result.written_lines == [app_line(1)]
        cursor = collector.source_cursor("app")
        # 游标正好落在第二条合格原行的行首。
        assert cursor == len((BAD_HEADER + "\n\n" + app_line(1) + "\n").encode())
        assert payload[cursor:cursor + 1] == b"2"
        assert payload[cursor - 1:cursor] == b"\n"

    def test_chinese_bytes_not_split(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        collector.claim("app")
        collector.settle_round()
        cursor = collector.source_cursor("app")
        data = (app_line(1) + "\n" + app_line(2) + "\n").encode()
        assert cursor == len((app_line(1) + "\n").encode())
        # 从游标继续解码一定得到完整字符，不会出现乱码。
        assert data[cursor:].decode("utf-8").startswith(app_line(2))
        assert not data[cursor:cursor + 1].startswith(b"\x80".hex().encode())

    def test_cursor_only_at_line_boundaries(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", "\n".join(
            [BAD_HEADER, "", app_line(1), app_line(2), app_line(3)]).encode() + b"\n")
        collector.begin_round()
        collector.claim("app")
        collector.claim("app")
        collector.settle_round()
        data = "\n".join(
            [BAD_HEADER, "", app_line(1), app_line(2), app_line(3)]).encode() + b"\n"
        cursor = collector.source_cursor("app")
        assert cursor == 0 or data[cursor - 1:cursor] == b"\n"

    def test_incomplete_tail_not_read_until_next_batch(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        first = app_line(1) + "\n"
        second_bytes = (app_line(2) + "\n").encode()
        collector.feed("app", first.encode() + second_bytes[:10])
        collector.begin_round()
        collector.claim("app")
        result = collector.settle_round()
        assert result.written_lines == [app_line(1)]
        # 半个字符留在缓冲里，游标停在第二条行首。
        assert collector.source_cursor("app") == len(first.encode())
        collector.begin_round()
        collector.feed("app", second_bytes[10:])
        collector.claim("app")
        second = collector.settle_round()
        assert second.written_lines == [app_line(2)]
        assert collector.reconcile("app")["sources"]["app"]["progress_matches_inventory"]

    def test_crlf_line_endings_counted_in_range(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        payload = (app_line(1) + "\r\n" + app_line(2) + "\r\n").encode()
        collector.feed("app", payload)
        collector.begin_round()
        collector.claim("app")
        collector.settle_round()
        cursor = collector.source_cursor("app")
        assert cursor == len((app_line(1) + "\r\n").encode())
        assert payload[cursor - 2:cursor] == b"\r\n"

    def test_scan_from_mid_line_falls_back_to_line_start(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        payload = (app_line(1) + "\n" + app_line(2) + "\n").encode()
        collector.feed("app", payload)
        mid = len((app_line(1) + "\n").encode()) + 5
        entries = collector.pipeline.read_source_lines(
            collector._sources["app"].source, payload, start=mid)
        # 不会从半个行/半个字符开读，而是回到该行行首。
        assert entries[0].raw_line == app_line(2)
        assert entries[0].byte_start == len((app_line(1) + "\n").encode())


class TestDuplicateIdempotency:
    """重复提交同一条原行只记一次名额与一次入库。"""

    def test_resubmitting_same_entry_does_not_consume_quota(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n").encode())
        collector.begin_round()
        first = collector.claim("app")
        again = collector.claim("app", first.entry)
        assert again is first
        assert collector.duplicate_submissions == 1
        result = collector.settle_round()
        assert result.written_lines == [app_line(1)]
        assert len(collector.inventory("app")) == 1

    def test_identical_bytes_refed_are_not_resent(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        collector.claim("app")
        first = collector.settle_round()
        assert first.written_lines == [app_line(1)]

        # 同一批原行被再次提交（同一字节范围）：只命中已有认领，不再占名额。
        entries_by_key = {e.entry_key: e for e in collector._sources["app"].entries}
        confirmed_entry = collector.confirmed_claims()[0].entry
        assert confirmed_entry.entry_key in entries_by_key
        collector.begin_round()
        again = collector.claim("app", confirmed_entry)
        assert again is collector.get_claim(confirmed_entry.entry_key)
        assert collector.duplicate_submissions == 1
        # 本轮继续领取真正的下一条原行 P2，重复提交的 P1 不占名额。
        collector.claim("app")
        result = collector.settle_round()
        assert app_line(1) not in result.written_lines
        assert result.written_lines == [app_line(2)]
        assert len(collector.inventory("app")) == 2

    def test_error_line_not_double_registered(self):
        collector = build_collector(BorrowPolicy.BORROW_ON_CLAIM)
        payload = (BAD_HEADER + "\n" + access_line(1) + "\n").encode()
        collector.feed("gateway", payload)
        collector.begin_round()
        collector.claim("gateway")
        collector.settle_round()
        # 重放同一段字节（同一字节范围）不应该把错误头再登记一次。
        collector.feed("gateway", (BAD_HEADER + "\n").encode())
        collector.begin_round()
        collector.settle_round()
        collector.feed("gateway", (BAD_HEADER + "\n" + access_line(1) + "\n").encode())
        collector.begin_round()
        collector.settle_round()
        assert len(collector.errors_for("gateway")) == 1


class TestConfirmationRecovery:
    """入库成功但回传确认失败时允许同一轮次断点恢复。"""

    def test_confirmation_failure_then_resume(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        collector.claim("app")
        collector.claim("app")

        attempts = {"count": 0}

        def flaky_ack(claim):
            attempts["count"] += 1
            if attempts["count"] <= 2:
                raise RuntimeError("回传确认超时")

        # 第一轮结算：入库成功，回传确认失败（停在“已入库待确认”）。
        result = collector.settle_round(flaky_ack)
        assert [claim.raw_line for claim in result.written] == [
            app_line(1), app_line(2)
        ]
        assert {claim.status for claim in result.written} == {ClaimStatus.INGESTED}
        assert len(result.confirmation_failures) == 2
        assert [claim.raw_line for claim in collector.ingested_claims()] == [
            app_line(1), app_line(2)
        ]
        progress_before = dict(collector.source_progress())

        # 同一轮次断点恢复：只重试确认，不重复入库。
        resumed = collector.resume_pending(lambda claim: None)
        assert resumed.confirmation_failures == []
        assert {claim.status for claim in collector.confirmed_claims()} == {
            ClaimStatus.CONFIRMED
        }
        assert collector.source_progress() == progress_before
        assert len(collector.inventory("app")) == 2
        assert collector.reconcile("app")["ok"] is True

    def test_failed_ack_keeps_ingested_state(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n").encode())
        collector.begin_round()
        collector.claim("app")

        def always_fail(claim):
            raise RuntimeError("ack failed")

        result = collector.settle_round(always_fail)
        assert len(result.written) == 1
        claim = result.written[0]
        assert claim.status == ClaimStatus.INGESTED
        assert claim.confirmation_error == "ack failed"
        assert claim.record_ids  # 已经真正入账
        assert len(collector.inventory("app")) == 1

        # 再恢复一次确认成功后，来源进度与实际入账原行仍然一致。
        claim = collector.retry_confirmation(claim, lambda item: None)
        assert claim.status == ClaimStatus.CONFIRMED
        assert collector.reconcile("app")["ok"] is True

    def test_resume_does_not_duplicate_inventory(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n").encode())
        collector.begin_round()
        collector.claim("app")
        collector.settle_round(lambda claim: (_ for _ in ()).throw(RuntimeError("x")))
        for _ in range(3):
            collector.resume_pending(lambda claim: None)
        assert len(collector.inventory("app")) == 1
        assert collector.reconcile("app")["ok"] is True


class TestReconciliation:
    """来源进度与库存原行一一对应。"""

    def test_reconcile_detects_progress_inventory_match(self):
        collector = build_collector(BorrowPolicy.BORROW_ON_CLAIM)
        collector.feed("gateway", (BAD_HEADER + "\n").encode())
        collector.feed("app", "".join(app_line(n) + "\n" for n in range(1, 5)).encode())
        for _ in range(2):
            collector.begin_round()
            collector.claim("app")
            collector.claim("app")
            collector.claim("app")
            collector.settle_round()
        report = collector.reconcile()
        assert report["ok"] is True
        for source in report["sources"].values():
            assert source["progress"] == source["job_cursor"]
            assert source["ingested_lines"] == source["inventory_lines"]

    def test_ledger_rows_all_confirmed(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", (app_line(1) + "\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        collector.claim("app")
        collector.claim("app")
        collector.settle_round()
        rows = collector.ledger(confirmed_only=True)
        assert len(rows) == 2
        assert all(row["status"] == "confirmed" for row in rows)
        assert all(row["record_ids"] for row in rows)


class TestSourceCursorEntryStates:
    """内部状态：错行/空行不占名额，但仍保留可恢复位置。"""

    def test_empty_line_does_not_consume_quota(self):
        collector = build_collector(BorrowPolicy.RESERVE_UNTIL_SUBMIT)
        collector.feed("app", ("\n" + app_line(1) + "\n\n" + app_line(2) + "\n").encode())
        collector.begin_round()
        first = collector.claim("app")
        second = collector.claim("app")
        assert first and second
        assert (first.line_number, second.line_number) == (2, 4)
        result = collector.settle_round()
        assert result.written_lines == [app_line(1), app_line(2)]
        # 空行状态为 consumed，且从未出现在错行清单。
        assert collector.errors_for("app") == []

    def test_factory_helper(self):
        collector = make_shared_quota_collector(policy="borrow_on_claim")
        assert collector.policy == BorrowPolicy.BORROW_ON_CLAIM
        assert collector.total_quota == 3


# ---------- 运行入口（确定性测试，不依赖 pytest） ----------

if __name__ == "__main__":
    import traceback

    tests = {
        "scenario_borrow_on_claim_p1_p2_p3": TestScenarioBothPolicies().test_borrow_on_claim_writes_p1_p2_p3,
        "scenario_reserve_p1_p2_g1": TestScenarioBothPolicies().test_reserve_until_submit_writes_p1_p2_g1,
        "scenario_late_line_once": TestScenarioBothPolicies().test_late_line_exists_only_once_in_inventory,
        "scenario_error_registered_once": TestScenarioBothPolicies().test_error_header_registered_only_once,
        "quota_total_three": TestQuotaAllocation().test_total_quota_is_three,
        "quota_base_per_source": TestQuotaAllocation().test_base_quota_per_source,
        "quota_donor_needs_no_line": TestQuotaAllocation().test_donor_must_have_no_qualified_line,
        "quota_no_donor_when_all_have_lines": TestQuotaAllocation().test_no_donor_when_all_sources_have_lines,
        "cursor_error_empty_advance": TestByteAccurateCursor().test_error_and_empty_lines_advance_cursor,
        "cursor_chinese_not_split": TestByteAccurateCursor().test_chinese_bytes_not_split,
        "cursor_line_boundaries": TestByteAccurateCursor().test_cursor_only_at_line_boundaries,
        "cursor_incomplete_tail": TestByteAccurateCursor().test_incomplete_tail_not_read_until_next_batch,
        "cursor_crlf_range": TestByteAccurateCursor().test_crlf_line_endings_counted_in_range,
        "cursor_mid_line_fallback": TestByteAccurateCursor().test_scan_from_mid_line_falls_back_to_line_start,
        "dup_same_entry": TestDuplicateIdempotency().test_resubmitting_same_entry_does_not_consume_quota,
        "dup_identical_bytes": TestDuplicateIdempotency().test_identical_bytes_refed_are_not_resent,
        "dup_error_once": TestDuplicateIdempotency().test_error_line_not_double_registered,
        "confirm_failure_resume": TestConfirmationRecovery().test_confirmation_failure_then_resume,
        "confirm_failed_state": TestConfirmationRecovery().test_failed_ack_keeps_ingested_state,
        "confirm_no_duplicate": TestConfirmationRecovery().test_resume_does_not_duplicate_inventory,
        "reconcile_match": TestReconciliation().test_reconcile_detects_progress_inventory_match,
        "reconcile_ledger": TestReconciliation().test_ledger_rows_all_confirmed,
        "state_empty_line_no_quota": TestSourceCursorEntryStates().test_empty_line_does_not_consume_quota,
        "state_factory": TestSourceCursorEntryStates().test_factory_helper,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_shared_quota: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_shared_quota: {name}")
            traceback.print_exc()

    print(f"\ntest_shared_quota: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)
