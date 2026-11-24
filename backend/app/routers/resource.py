"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    DispatchPayload,
    EntryPayload,
    PageResult,
    WindowReportPayload,
)
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


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
    accepted = set(service.LIST_FIELDS) | set(getattr(service, "QUERY_FIELDS", []))
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in accepted and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    accepted = set(service.LIST_FIELDS) | set(getattr(service, "QUERY_FIELDS", []))
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in accepted and value.strip()
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


def _verdict_verifier(verdicts: dict[str, Any]):
    """把上报的逐段核验结论包装成 verifier；未列出的片段视为来源一致。"""
    if not verdicts:
        return None

    def verifier(segment):
        raw = verdicts.get(segment.start_iso)
        if raw is None:
            return True, "来源一致"
        if isinstance(raw, dict):
            return bool(raw.get("ok")), str(raw.get("reason", ""))
        return bool(raw), ""

    return verifier


# ── 来源窗口迁移专用接口 ──────────────────────────────────────
# 这些路径必须注册在通用 /{module}/{entry_id} 之前，否则会被当成 entry_id。


@router.post("/window_migration/report", response_model=dict)
def report_source_window(payload: WindowReportPayload) -> dict[str, Any]:
    """机器人补报精确来源窗口；缺时间或时区直接 400，绝不猜填。"""
    from app.source_window import WindowError

    service = require_service("window_migration")
    try:
        result = service.report_window(
            payload.values.get("批次编号") or payload.values.get("关联批次"),
            payload.values,
            operator=payload.operator,
        )
    except WindowError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if payload.verify and result.get("status") == "created":
        migration = result["migration"]
        result["verification"] = service.verify(
            migration["id"],
            verifier=_verdict_verifier(payload.verdicts),
            interrupt_after=payload.interrupt_after,
            operator=payload.operator,
        )
    return result


@router.post("/window_migration/{entry_id}/verify", response_model=dict)
def verify_source_window(entry_id: int, payload: WindowReportPayload) -> dict[str, Any]:
    """核验迁移；``interrupt_after`` 可模拟当晚只核实午夜前一段就中断。"""
    service = require_service("window_migration")
    return service.verify(
        entry_id,
        verifier=_verdict_verifier(payload.verdicts),
        interrupt_after=payload.interrupt_after,
        operator=payload.operator,
    )


@router.post("/window_migration/{entry_id}/resume", response_model=dict)
def resume_source_window(entry_id: int, payload: WindowReportPayload) -> dict[str, Any]:
    """恢复中断的迁移，只处理未核验/失败片段。"""
    service = require_service("window_migration")
    return service.resume(
        entry_id,
        verifier=_verdict_verifier(payload.verdicts),
        operator=payload.operator,
    )


@router.get("/window_migration/{entry_id}/summary", response_model=dict)
def source_window_summary(entry_id: int) -> dict[str, Any]:
    """一次返回迁移、原批次、已交付交接、回执与补采任务，供值班员回查。"""
    service = require_service("window_migration")
    result = service.summary(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"窗口迁移 {entry_id} 不存在或已归档")
    return result


@router.post("/backfill_queue/{entry_id}/dispatch", response_model=dict)
def dispatch_backfill(entry_id: int, payload: DispatchPayload) -> dict[str, Any]:
    """按迁移核验结果派发补采区间；重复尝试不会重复派发。"""
    service = require_service("backfill_queue")
    return service.attempt_dispatch(
        entry_id, operator=payload.operator, node=payload.node
    )
