"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


class FeedSourcePayload(BaseModel):
    source_key: str
    content: str


class RoundPayload(BaseModel):
    rounds: int = Field(default=1, ge=1, le=20)
    confirm: bool = True


class SourceRegisterPayload(BaseModel):
    source_key: str
    order: int | None = None
    app_label: str | None = None


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


# ---------- 采集作业：跨来源共享额度采集 ----------

def _ingest_service():
    service = service_for("ingest_job")
    if service is None:
        raise HTTPException(status_code=404, detail="采集作业服务未注册")
    return service


@router.get("/ingest_job/shared-quota", response_model=dict)
def shared_quota_snapshot() -> dict[str, Any]:
    """跨来源额度、入账清单、错行登记与来源进度对账。"""
    return _ingest_service().shared_quota_snapshot()


@router.post("/ingest_job/sources", response_model=dict)
def register_ingest_source(payload: SourceRegisterPayload) -> dict[str, Any]:
    """登记一个采集来源；来源顺序决定各轮领取优先级。"""
    try:
        return _ingest_service().register_source(
            payload.source_key, order=payload.order, app_label=payload.app_label)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/ingest_job/feed", response_model=dict)
def feed_ingest_source(payload: FeedSourcePayload) -> dict[str, Any]:
    """向来源投递原始日志（含行尾的字节/文本），下一轮从第一条未确认字节继续。"""
    try:
        return _ingest_service().feed_source(payload.source_key, payload.content)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/ingest_job/collect", response_model=dict)
def collect_shared_quota(payload: RoundPayload) -> dict[str, Any]:
    """按来源顺序结算若干轮，驱动入账清单并写入库存。"""
    return _ingest_service().collect_round(payload.rounds, payload.confirm)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
