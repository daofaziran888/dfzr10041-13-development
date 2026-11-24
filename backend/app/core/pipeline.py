"""拾音工坊核心管道：log_source→parser_rule→ingest_job→event_store 端到端采集解析链路。

数据流：原始日志行 → 格式校验 → 正则解析 → 字段提取 → 事件存储
涉及模块：log_source, parser_rule, ingest_job, event_store, batch_checkpoint
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.core.models import (
    BatchCheckpoint, EventRecord, EventTypeDef, IngestJob, IngestMetric,
    LogEntry, LogFormat, LogSourceConfig, ParseResultStatus, ParseRule,
    QualityReport,
)

# ---------- 预置日志来源配置 ----------

PRESET_LOG_SOURCES: dict[str, LogSourceConfig] = {
    "access-web": LogSourceConfig(
        source_id="LOG_-0001",
        source_name="WebAccessLog",
        app_label="web-gateway",
        log_path="/var/log/nginx/access.log",
        log_format="access",
        encoding="utf-8",
        collect_method="file_tail",
        access_state="已接入",
    ),
    "app-backend": LogSourceConfig(
        source_id="LOG_-0002",
        source_name="BackendAppLog",
        app_label="backend-api",
        log_path="/var/log/app/backend.log",
        log_format="app",
        encoding="utf-8",
        collect_method="file_tail",
        access_state="已接入",
    ),
    "sys-auth": LogSourceConfig(
        source_id="LOG_-0003",
        source_name="SysAuthLog",
        app_label="auth-service",
        log_path="/var/log/auth/syslog",
        log_format="sys",
        encoding="utf-8",
        collect_method="file_tail",
        access_state="已接入",
    ),
}

# ---------- 预置解析规则 ----------

PRESET_PARSE_RULES: list[ParseRule] = [
    ParseRule(
        rule_id="PARS-0001",
        source_id="LOG_-0001",
        regex_pattern=r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+(\S+)\s+HTTP/\d\.\d"\s+(\d{3})\s+(\d+)\s+"([^"]*)"\s+"([^"]*)"$',
        field_names=["client_ip", "time_local", "method", "url", "status", "body_bytes", "referer", "user_agent"],
        time_format="%d/%b/%Y:%H:%M:%S %z",
        priority=10,
        state="生效中",
    ),
    ParseRule(
        rule_id="PARS-0002",
        source_id="LOG_-0002",
        regex_pattern=r'^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})\s+\[([^\]]+)\]\s+(INFO|WARN|ERROR|DEBUG)\s+(\S+)\s+-\s+(.*)$',
        field_names=["timestamp", "thread", "level", "logger", "message"],
        time_format="%Y-%m-%d %H:%M:%S.%f",
        priority=10,
        state="生效中",
    ),
    ParseRule(
        rule_id="PARS-0003",
        source_id="LOG_-0003",
        regex_pattern=r'^([\w-]+\s+\d+\s+\d{2}:\d{2}:\d{2})\s+([\w-]+)\s+([\w-]+)\[(\d+)\]:\s+(\w+)\s+(.*)$',
        field_names=["sys_time", "hostname", "process", "pid", "action", "detail"],
        time_format="%b %d %H:%M:%S",
        priority=10,
        state="生效中",
    ),
]

# ---------- 预置事件类型 ----------

PRESET_EVENT_TYPES: dict[str, EventTypeDef] = {
    "access": EventTypeDef(
        type_id="EVEN-0001", type_name="访问事件",
        category_tag="access", severity="INFO",
        sample_ratio=0.5, retain_days=30,
    ),
    "error": EventTypeDef(
        type_id="EVEN-0002", type_name="错误事件",
        category_tag="error", severity="HIGH",
        sample_ratio=1.0, retain_days=90,
    ),
    "auth": EventTypeDef(
        type_id="EVEN-0003", type_name="认证事件",
        category_tag="auth", severity="MEDIUM",
        sample_ratio=1.0, retain_days=60,
    ),
}


@dataclass
class IngestPipelineState:
    """采集管道运行时状态——跨模块数据流的中间产物。"""
    source: LogSourceConfig
    rule: ParseRule
    job: IngestJob
    parsed_logs: list[LogEntry]
    events: list[EventRecord]
    errors: list[dict[str, Any]]
    checkpoints: list[BatchCheckpoint]
    metric: IngestMetric
    quality_report: QualityReport | None = None


class IngestPipeline:
    """核心采集管道：log_source → parser_rule → ingest_job → event_store。

    单次管道执行：
    1. 从 log_source 获取原始日志行
    2. 使用 parser_rule 逐行解析
    3. 通过 ingest_job 控制采集批次
    4. 生成 EventRecord 写入 event_store
    5. 产出 batch_checkpoint 支持断点恢复
    6. 产出 ingest_metric 作为质量报告的数据源
    """

    def __init__(self):
        self._checkpoint_store: dict[str, BatchCheckpoint] = {}
        self._event_store: dict[str, list[EventRecord]] = {}
        self._fingerprint_seen: set[str] = set()
        self._classified_keys: set[str] = set()
        self._stored_entry_keys: set[str] = set()

    # ---------- 数据流 1: log_source → parser_rule → ingest_job → event_store ----------

    def parse_raw_lines(
        self,
        source: LogSourceConfig,
        raw_lines: list[str],
        *,
        start_offset: int = 0,
        encoding: str | None = None,
    ) -> list[LogEntry]:
        """用来源配置校验原始日志行格式。

        每一行都会带上原始字节范围：``byte_start`` 含头不含尾、``byte_end``
        包含行尾换行符。空行和格式错行也保留位置，供断点恢复使用；
        它们不参与采集名额结算，但下一轮必须能从它们之后继续。
        """
        encoding = encoding or source.encoding or "utf-8"
        entries: list[LogEntry] = []
        offset = start_offset
        for i, line in enumerate(raw_lines):
            text = line
            payload = text.encode(encoding)
            terminated = True
            if payload.endswith(b"\n"):
                # 调用方已经带上了行尾，原样保留字节范围。
                content_bytes = payload[:-1]
                if content_bytes.endswith(b"\r"):
                    content_bytes = content_bytes[:-1]
            else:
                payload += b"\n"
                content_bytes = payload[:-1]
            try:
                text = content_bytes.decode(encoding)
                decode_error = False
            except UnicodeDecodeError:
                text = content_bytes.decode(encoding, errors="replace")
                decode_error = True
            byte_start = offset
            byte_end = offset + len(payload)
            offset = byte_end
            if decode_error:
                status = ParseResultStatus.ENCODING_ERROR
            elif not text.strip():
                status = ParseResultStatus.LINE_EMPTY
            elif not source.match_format(text):
                status = ParseResultStatus.FORMAT_MISMATCH
            else:
                status = ParseResultStatus.OK
            entry = LogEntry(
                raw_line=text,
                line_number=i + 1,
                source_id=source.source_id,
                parse_status=status,
                byte_start=byte_start,
                byte_end=byte_end,
                raw_bytes=payload,
                terminated=terminated,
                encoding=encoding,
            )
            entries.append(entry)
        return entries

    # ---------- 按字节读取来源文件（含行尾、编码安全） ----------

    def read_source_lines(
        self,
        source: LogSourceConfig,
        data: bytes,
        *,
        start: int = 0,
        encoding: str | None = None,
        rule: ParseRule | None = None,
        include_partial: bool = True,
    ) -> list[LogEntry]:
        """从来源原始字节的 ``start`` 位置逐行读取。

        - 行边界按 ``\\n`` 切分，字节范围包含行尾；``\\r\\n`` 也完整计入。
        - 使用来源的真实编码逐行解码，绝不从半个多字符处开始。
        - 若 ``start`` 落在行中间，会回退到最近的换行之后重新读取（重复行由
          ``entry_key`` 幂等去重），避免半个字符/半行。
        - 文件末尾没有换行的残行默认标记为未终结并保留；协调器会等下一批
          字节到达后再确认它。
        """
        encoding = encoding or source.encoding or "utf-8"
        if start < 0 or start > len(data):
            raise ValueError(f"读取位置越界：{start}")
        if start > 0 and data[start - 1:start] != b"\n":
            previous_newline = data.rfind(b"\n", 0, start)
            start = previous_newline + 1 if previous_newline >= 0 else 0
        line_number = data.count(b"\n", 0, start) + 1
        entries: list[LogEntry] = []
        position = start
        while position < len(data):
            newline = data.find(b"\n", position)
            if newline < 0:
                if not include_partial:
                    break
                byte_end = len(data)
                payload = data[position:byte_end]
                content_bytes = payload
                terminated = False
            else:
                byte_end = newline + 1
                payload = data[position:byte_end]
                content_bytes = data[position:newline]
                terminated = True
            if content_bytes.endswith(b"\r"):
                content_bytes = content_bytes[:-1]
            try:
                text = content_bytes.decode(encoding)
                decode_error = False
            except UnicodeDecodeError:
                text = content_bytes.decode(encoding, errors="replace")
                decode_error = True
            if decode_error:
                status = ParseResultStatus.ENCODING_ERROR
            elif not text.strip():
                status = ParseResultStatus.LINE_EMPTY
            elif not source.match_format(text):
                status = ParseResultStatus.FORMAT_MISMATCH
            else:
                status = ParseResultStatus.OK
            entry = LogEntry(
                raw_line=text,
                line_number=line_number,
                source_id=source.source_id,
                parse_status=status,
                byte_start=position,
                byte_end=byte_end,
                raw_bytes=payload,
                terminated=terminated,
                encoding=encoding,
            )
            if rule is not None and status == ParseResultStatus.OK:
                parsed_status, fields = rule.parse(text)
                entry.parse_status = parsed_status
                entry.parsed_fields = fields or {}
                entry.checksum = entry.compute_checksum()
            entries.append(entry)
            position = byte_end
            line_number += 1
        return entries

    def read_source_batch(self, source: LogSourceConfig, data: bytes, **kwargs: Any) -> list[LogEntry]:
        """``read_source_lines`` 的别名，便于按批次读取来源日志。"""
        return self.read_source_lines(source, data, **kwargs)

    def apply_parse_rules(self, entries: list[LogEntry], rule: ParseRule) -> list[LogEntry]:
        """用解析规则从日志条目中提取字段。"""
        for entry in entries:
            if entry.parse_status != ParseResultStatus.OK:
                continue
            status, fields = rule.parse(entry.raw_line)
            entry.parse_status = status
            entry.parsed_fields = fields or {}
            entry.checksum = entry.compute_checksum()
        return entries

    def run_ingest_job(
        self,
        entries: list[LogEntry],
        job: IngestJob,
        *,
        enforce_batch_size: bool = True,
    ) -> list[LogEntry]:
        """执行采集作业：按 batch_size 分批，追踪文件游标和跳过行数。

        只有合格原行（``ParseResultStatus.OK``）消耗采集名额；空行、格式错行、
        编码错行只登记跳过，但仍然推进到包含行尾的字节位置。游标始终指向
        “第一条未确认的原始字节”，不会因为按成功行数累加而越过错误行。
        """
        processed: list[LogEntry] = []
        seen = job.seen_entry_keys
        for entry in entries:
            key = entry.entry_key or entry.compute_entry_key()
            # 没有字节位置的条目（手工构造/测试数据）按当前游标补齐。
            if entry.byte_end <= entry.byte_start:
                entry.ensure_positions(job.file_cursor, job.encoding)
                key = entry.entry_key or key
            if key in seen:
                # 已确认过的原行不重复占名额，但恢复位置仍需越过它。
                job.duplicate_lines += 1
                job.file_cursor = max(job.file_cursor, entry.byte_end)
                continue
            qualified = entry.parse_status == ParseResultStatus.OK
            if qualified and enforce_batch_size and job.processed_lines >= job.batch_size:
                # 停在第一条未确认的合格原行开头，下一轮从这里继续读。
                break
            if not qualified:
                job.skipped_lines += 1
                job.file_cursor = max(job.file_cursor, entry.byte_end)
                seen.add(key)
                processed.append(entry)
                continue
            job.processed_lines += 1
            job.file_cursor = max(job.file_cursor, entry.byte_end)
            seen.add(key)
            processed.append(entry)
        return processed

    def classify_events(self, entries: list[LogEntry], *, dedupe: bool = True) -> list[EventRecord]:
        """将解析后的日志条目按事件类型分类并生成 EventRecord。

        数据流入口：event_store ← 解析后的 LogEntry
        分类依据：日志级别（app log → error/access）或来源标识
        ``entry_key`` 保证重复提交同一条原行只生成一条库存事件。
        """
        records: list[EventRecord] = []
        for entry in entries:
            if entry.parse_status != ParseResultStatus.OK or not entry.parsed_fields:
                continue
            key = entry.entry_key or entry.compute_entry_key()
            if dedupe and key in self._classified_keys:
                continue
            event_type = self._classify_entry(entry)
            host = entry.parsed_fields.get("client_ip") or entry.parsed_fields.get("hostname") or "unknown"
            partition = f"{datetime.now():%Y%m%d}"
            digest = hashlib.sha1(
                f"{entry.source_id}|{key}".encode()
            ).hexdigest()[:12].upper()
            record = EventRecord(
                record_id=f"EVEN-{digest}",
                event_type=event_type,
                original_content=entry.raw_line[:500],
                parsed_fields=dict(entry.parsed_fields),
                masked_fields={},
                enriched_fields={},
                event_time=entry.event_time or datetime.now(),
                source_host=host,
                store_partition=partition,
                index_state="已写入",
                entry_key=key,
            )
            if dedupe:
                self._classified_keys.add(key)
            records.append(record)
        return records

    def store_events(self, records: list[EventRecord]) -> int:
        """写入 event_store 并返回存储条数。

        同一 ``entry_key`` 重复入库只保留一条事件记录。
        """
        stored = 0
        for record in records:
            key = record.entry_key or record.record_id
            if key in self._stored_entry_keys:
                continue
            self._stored_entry_keys.add(key)
            self._event_store.setdefault(record.source_host, []).append(record)
            stored += 1
        return stored

    # ---------- 数据流 2: ingest_job → batch_checkpoint（断点恢复） ----------

    def create_checkpoint(self, job: IngestJob, file_path: str, lines: list[str]) -> BatchCheckpoint:
        """为采集作业创建批次检查点——支持断点恢复。

        记录：文件偏移量、已处理行数、内容校验哈希（用最后 N 行拼接）。
        """
        last_blob = "".join(l.raw_line for l in lines[-5:]) if lines else ""
        content_hash = hashlib.sha256(
            f"{file_path}|{job.file_cursor}|{last_blob}".encode()
        ).hexdigest()[:16]
        checkpoint = BatchCheckpoint(
            checkpoint_id=f"BATC-{job.job_id}-{job.processed_lines:06d}",
            job_id=job.job_id,
            file_path=file_path,
            byte_offset=job.file_cursor,
            line_count=job.processed_lines + job.skipped_lines,
            content_hash=content_hash,
            state="已记录",
        )
        self._checkpoint_store[checkpoint.checkpoint_id] = checkpoint
        return checkpoint

    def resume_from_checkpoint(self, checkpoint: BatchCheckpoint) -> int:
        """从检查点恢复采集：返回应从此偏移量继续的文件位置。"""
        return checkpoint.byte_offset

    # ---------- 数据流 4: event_type → dedup_filter → sample_job ----------

    def dedup_events(self, records: list[EventRecord], dedup_fields: list[str]) -> list[EventRecord]:
        """对事件记录集合执行去重过滤——对应 dedup_filter 模块。

        使用 SHA256 指纹算法，基于指定的去重字段生成指纹。
        """
        seen: set[str] = set()
        unique: list[EventRecord] = []
        for record in records:
            key_parts = [record.event_type.type_id]
            for field in sorted(dedup_fields):
                val = record.parsed_fields.get(field, "")
                key_parts.append(f"{field}={val}")
            fp = hashlib.sha256("|".join(key_parts).encode()).hexdigest()
            if fp in seen:
                continue
            seen.add(fp)
            unique.append(record)
        return unique

    def sample_events(self, records: list[EventRecord], sample_ratio: float,
                      merge_key: str = "") -> list[EventRecord]:
        """采样归并——对应 sample_job 模块。

        按 sample_ratio 采样，按 merge_key 归并（同 key 仅保留最新）。
        """
        sampled: list[EventRecord] = []
        merged: dict[str, EventRecord] = {}
        threshold = int(len(records) * sample_ratio)
        for i, record in enumerate(records):
            if merge_key:
                mkey = record.parsed_fields.get(merge_key, "")
                if mkey:
                    merged[mkey] = record
            if i < threshold:
                sampled.append(record)
        if merge_key:
            return list(merged.values())
        return sampled

    # ---------- 数据流 6: archive_policy → event_store ----------

    def apply_archive_policy(self, partition: str, retain_days: int) -> list[str]:
        """归档策略：标记超过保留天数的分区为可归档。"""
        to_archive: list[str] = []
        cutoff_date = datetime.now() - __import__("datetime").timedelta(days=retain_days)
        cutoff_int = int(cutoff_date.strftime("%Y%m%d"))
        for host, records in self._event_store.items():
            for record in records:
                try:
                    partition_int = int(record.store_partition) if record.store_partition else 99999999
                except ValueError:
                    continue
                if partition_int < cutoff_int:
                    to_archive.append(record.record_id)
                    record.index_state = "已归档"
        return to_archive

    # ---------- 数据流 7: schema_version → parser_rule ----------

    def detect_schema_change(self, old_fields: list[str], new_fields: list[str]) -> dict[str, Any]:
        """检测Schema版本变更对解析规则的影响。

        返回变更详情：兼容/不兼容判定、新增/删除字段清单、影响事件数。
        """
        old_set = set(old_fields or [])
        new_set = set(new_fields or [])
        added = list(new_set - old_set)
        removed = list(old_set - new_set)
        is_compatible = len(removed) == 0
        affected = sum(
            len(records)
            for records in self._event_store.values()
        )
        return {
            "added_fields": added,
            "removed_fields": removed,
            "is_compatible": is_compatible,
            "affected_event_count": affected,
            "version_time": datetime.now().isoformat(),
        }

    # ---------- 数据流 8: ingest_job → ingest_metric → quality_report ----------

    def compute_ingest_metrics(self, job: IngestJob, elapsed_ms: float,
                                total_lines: int) -> IngestMetric:
        """从采集作业运行状态计算采集指标。

        采集速率 = 处理行数 / 耗时(s)
        错误率 = 跳过行数 / 总行数
        积压行数 = 总行数 - 处理行数
        """
        elapsed_s = max(elapsed_ms / 1000.0, 0.001)
        rate = job.processed_lines / elapsed_s
        error_rate = job.skipped_lines / total_lines if total_lines > 0 else 0.0
        backlog = total_lines - job.processed_lines - job.skipped_lines
        return IngestMetric(
            metric_id=f"INGE-{job.job_id}-{int(time.time())}",
            source_id=job.source_id,
            ingest_rate=round(rate, 2),
            process_delay_ms=round(elapsed_ms, 2),
            error_rate=round(error_rate, 4),
            backlog_lines=max(backlog, 0),
        )

    def generate_quality_report(self, metric: IngestMetric,
                                 parse_ok: int, parse_total: int,
                                 fields_coverage: float,
                                 processed: int = 0,
                                 skipped: int = 0) -> QualityReport:
        """从采集指标生成质量报告——对应 quality_report 模块。

        完整率 = 处理行数 / (处理+跳过)
        解析成功率 = 成功解析行数 / 总处理行数
        字段覆盖率 = 已填充字段数 / 期望字段总数
        """
        total = processed + skipped
        completeness = processed / total if total > 0 else 0.0
        parse_rate = parse_ok / parse_total if parse_total > 0 else 0.0
        return QualityReport(
            report_id=f"QUAL-{metric.metric_id}",
            source_id=metric.source_id,
            completeness_rate=round(completeness, 4),
            parse_success_rate=round(parse_rate, 4),
            field_coverage_rate=round(fields_coverage, 4),
            time_delay_seconds=metric.process_delay_ms / 1000.0,
            eval_period="hourly",
            state="已生成",
        )

    # ---------- 数据流 5: event_type → anomaly_rule → forward_rule ----------

    def check_anomaly(self, records: list[EventRecord], event_type_id: str,
                       threshold: int) -> list[EventRecord]:
        """异常检测：统计指定事件类型数量，触发告警条件。

        对应 anomaly_rule 模块：事件分类→异常告警路径。
        """
        matched = [r for r in records if r.event_type.type_id == event_type_id]
        if len(matched) > threshold:
            for r in matched:
                r.parsed_fields["anomaly_alert"] = "triggered"
        return matched

    def apply_forward_rules(self, records: list[EventRecord],
                             target: str) -> list[dict[str, Any]]:
        """编组转发规则：按目标地址组织事件批次——对应 forward_rule 模块。

        返回按目标地址分组的转发批次，供外部消费。
        """
        batches: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            key = target or "default"
            batches.setdefault(key, []).append({
                "record_id": record.record_id,
                "event_type": record.event_type.type_id,
                "parsed_fields": record.parsed_fields,
                "event_time": record.event_time.isoformat(),
            })
        return [
            {"target": k, "count": len(v), "payload": v}
            for k, v in batches.items()
        ]

    # ---------- 内部辅助方法 ----------

    def _classify_entry(self, entry: LogEntry) -> EventTypeDef:
        """根据解析字段对日志条目做事件分类。"""
        fields = entry.parsed_fields or {}
        level = fields.get("level", "").upper()
        if level == "ERROR":
            return PRESET_EVENT_TYPES.get("error", PRESET_EVENT_TYPES["access"])
        action = fields.get("action", "").upper()
        if action in ("LOGIN", "LOGOUT", "AUTH", "AUTHENTICATE"):
            return PRESET_EVENT_TYPES.get("auth", PRESET_EVENT_TYPES["access"])
        return PRESET_EVENT_TYPES.get("access", PRESET_EVENT_TYPES["access"])

    def get_event_store(self) -> dict[str, list[EventRecord]]:
        return self._event_store

    def get_checkpoints(self) -> list[BatchCheckpoint]:
        return list(self._checkpoint_store.values())


# ---------- 测试辅助工厂 ----------

def make_sample_raw_lines() -> list[str]:
    """生成用于确定测试的混合日志样本行（包含脏日志、格式不匹配的行）。"""
    return [
        # access 日志
        '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /api/users HTTP/1.1" 200 1234 "-" "Mozilla/5.0"',
        '10.0.0.55 - - [15/Nov/2026:14:30:06 +0800] "POST /api/events/ingest HTTP/1.1" 201 567 "-" "curl/7.88"',
        '172.16.0.3 - - [15/Nov/2026:14:30:07 +0800] "GET /api/health HTTP/1.1" 500 89 "-" "kube-probe/1.28"',
        # app 日志
        '2026-11-15 14:30:05.123 [http-nio-8080-exec-1] INFO  com.example.IngestController - 采集作业 INGE-0001 启动',
        '2026-11-15 14:30:05.456 [http-nio-8080-exec-2] ERROR com.example.ParserEngine - 解析失败：regex mismatch at line 88',
        '2026-11-15 14:30:05.789 [pool-3-thread-1] DEBUG com.example.CheckpointStore - 检查点 BATC-0001 写入成功',
        '2026-11-15 14:30:06.012 [http-nio-8080-exec-3] WARN  com.example.MaskService - 脱敏规则 MASK-0003 遗漏字段 user_email',
        # 脏日志（格式不匹配）
        '2026-11-15 14:30:06.234 orphan line without proper format',
        '',
        # app 日志 error（重复模式用于测试去重）
        '2026-11-15 14:30:06.456 [http-nio-8080-exec-2] ERROR com.example.ParserEngine - 解析失败：regex mismatch at line 89',
        # sys 日志
        'Nov 15 14:30:07 auth-server sshd[8891]: LOGIN user=admin from=10.0.0.100',
        'Nov 15 14:30:08 auth-server sshd[8892]: LOGIN user=operator from=192.168.1.50',
        'Nov 15 14:30:09 auth-server sshd[8893]: AUTH failure user=guest reason=password_expired',
        # access 日志（含敏感IP用于脱敏测试）
        '192.168.1.99 - - [15/Nov/2026:14:30:10 +0800] "GET /api/users/13800138000 HTTP/1.1" 200 2000 "-" "Mozilla/5.0"',
        # 脏行（纯数字）
        '2926-16-15 99:99:99.999 [bad] BAD  malformed - 完全不匹配任何格式',
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
    ]


def make_pipeline() -> IngestPipeline:
    """工厂函数：创建一个预配置的采集管道实例。"""
    return IngestPipeline()