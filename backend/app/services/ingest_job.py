"""采集作业领域服务：字段/状态来自业务配置，跨来源共享额度采集由领域层完成。"""
from __future__ import annotations

from typing import Any

from app.core.pipeline import PRESET_LOG_SOURCES
from app.core.shared_quota import (
    BorrowPolicy,
    CrossSourceQuotaCollector,
    make_shared_quota_collector,
)
from app.services.base import BaseModuleService


class IngestJobService(BaseModuleService):
    MODULE = "ingest_job"
    ENTITY = "采集任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["作业编号", "日志来源", "采集模式", "文件游标", "批次大小", "并发线程", "处理行数", "跳过行数"]
    REQUIRED_FIELDS = ["作业编号", "日志来源", "采集模式"]
    STATUS_ORDER = ["待执行", "采集中", "已完成", "部分失败"]
    ACTION_RULES = {"启动采集": "采集中", "暂停作业": "已完成", "标记完成": "部分失败"}
    NEGATIVE_ACTIONS = []

    # 来源 key → 预置来源配置，供接口按来源名登记采集来源。
    SOURCE_PRESETS = {
        "gateway": PRESET_LOG_SOURCES["access-web"],
        "app": PRESET_LOG_SOURCES["app-backend"],
        "auth": PRESET_LOG_SOURCES["sys-auth"],
    }

    def __init__(self) -> None:
        # 关键点：协调器挂在服务实例上，作业动作可连续领取多轮。
        self.collector: CrossSourceQuotaCollector = make_shared_quota_collector()

    def reset_collector(
        self,
        *,
        policy: BorrowPolicy | str = BorrowPolicy.RESERVE_UNTIL_SUBMIT,
        total_quota: int = 3,
        base_quota: int = 1,
    ) -> CrossSourceQuotaCollector:
        """重建协调器（测试/演示隔离用）。"""
        self.collector = make_shared_quota_collector(
            policy=policy, total_quota=total_quota, base_quota=base_quota
        )
        return self.collector

    def register_source(
        self,
        source_key: str,
        *,
        order: int | None = None,
        app_label: str | None = None,
    ) -> dict[str, Any]:
        """登记一个来源；未登记的来源用预置配置。"""
        preset = self.SOURCE_PRESETS.get(source_key)
        if preset is None:
            raise ValueError(f"未知采集来源：{source_key}")
        source = preset
        if app_label:
            source = type(preset)(**{**preset.__dict__, "app_label": app_label})
        self.collector.register_source(source_key, source, order=order)
        return {"source_key": source_key, "source_id": source.source_id}

    def feed_source(self, source_key: str, content: bytes | str) -> dict[str, Any]:
        """向来源追加原始日志字节/文本。"""
        self.collector.feed(source_key, content)
        return {"source_key": source_key, "received": len(content)}

    def collect_round(self, rounds: int = 1, confirm: bool = True) -> dict[str, Any]:
        """按来源顺序连续领取并结清若干轮。

        每轮先把三个名额发满：按来源顺序让各来源各取一格，名额还有剩余就回到
        来源顺序继续取（共享名额、借用名额都在 ``claim`` 内分配），直到没有
        来源还能领取为止。
        """
        collector = self.collector
        results: list[dict[str, Any]] = []
        for _ in range(max(rounds, 1)):
            collector.begin_round()
            while True:
                progressed = False
                for key in sorted(collector._sources, key=lambda k: collector._sources[k].order):
                    claim = collector.claim(key)
                    if claim is not None and claim.status.value == "claimed":
                        progressed = True
                if not progressed:
                    break
            hook = (lambda claim: None) if confirm else None
            result = collector.settle_round(hook)
            results.append({
                "round": result.round_index,
                "written": [claim.as_dict() for claim in result.written],
                "deferred": [claim.as_dict() for claim in result.deferred],
                "preempted": [claim.as_dict() for claim in result.preempted],
                "errors": [item.as_dict() for item in result.errors],
                "progress": result.source_progress,
            })
        return {
            "rounds": results,
            "reconcile": collector.reconcile(),
            "ledger": collector.ledger(),
        }

    def shared_quota_snapshot(self) -> dict[str, Any]:
        """跨来源额度与对账快照，供工作台/看板消费。"""
        collector = self.collector
        return {
            "total_quota": collector.total_quota,
            "base_quota": collector.base_quota,
            "policy": collector.policy.value,
            "round": collector.round_index,
            "sources": {
                key: {
                    "source_id": state.source.source_id,
                    "order": state.order,
                    "cursor": collector.source_cursor(key),
                    "processed_lines": state.job.processed_lines,
                    "skipped_lines": state.job.skipped_lines,
                    "duplicate_lines": state.job.duplicate_lines,
                }
                for key, state in collector._sources.items()
            },
            "error_lines": collector.error_ledger(),
            "ledger": collector.ledger(),
            "reconcile": collector.reconcile(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["shared_quota"] = self.shared_quota_snapshot()
        return snapshot
