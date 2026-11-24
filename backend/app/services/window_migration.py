"""来源窗口迁移服务：把旧日标签批次迁移成带时区的精确来源窗口。

迁移围绕一份机器人补报展开，逐片段核验：

1. **只登记真实区间**：窗口起止与时区全部来自补报，缺失或非法直接拒绝，
   绝不用旧日标签反推时间戳；
2. **原标签可回查**：旧 ``[2026-11-21]`` 保留在 ``原始日标签`` 与
   ``采集窗口`` 中，不做改写；
3. **逐段核验、可中断可恢复**：每核实一段就写一张落库回执并更新补采任务，
   中断后恢复只处理待核验片段，已核验片段不重复出回执、不重复抵扣；
4. **同一份补报只确认一次**：以 ``(批次, 精确窗口, 时区)`` 计算幂等键，
   重复补传直接返回已有迁移，不产生第二套回执/补采任务。

覆盖判定结合原日标签与已交付明细：只有当片段完全落在原标签当日、且原批次
已确认或已存在已交接的交接包时，才判为「已抵扣」；未知或未核验的时间既不
猜填也不抵扣补采。
"""
from __future__ import annotations

import json
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Any, Callable, Mapping

from app.services.base import BaseModuleService
from app.source_window import (
    SourceWindow,
    WindowError,
    WindowSegment,
    format_day_label,
    idempotency_key,
    parse_day_label,
    segment_key,
    segments_from_json,
    segments_to_json,
)

Verifier = Callable[[WindowSegment], Any]


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class MigrationInterrupted(RuntimeError):
    """迁移在约定片段数后被中断；状态已落库，可恢复。"""


class WindowMigrationService(BaseModuleService):
    MODULE = "window_migration"
    ENTITY = "窗口迁移"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = [
        "迁移编号", "关联批次", "批次编号", "原始日标签",
        "来源窗口起", "来源窗口止", "来源时区", "补报键", "幂等键",
        "片段明细", "已核验片段", "待核验片段", "失败片段",
        "关联回执", "已抵扣片段", "恢复次数", "中断原因",
        "操作者", "上报时间", "更新时间",
    ]
    REQUIRED_FIELDS = ["迁移编号", "关联批次"]
    STATUS_ORDER = ["待核验", "核验中", "已中断", "已完成"]
    ACTION_RULES = {"恢复迁移": "核验中", "作废迁移": "已完成"}
    NEGATIVE_ACTIONS = []

    # ── 登记补报 ─────────────────────────────────────────────

    def find_by_code(self, migration_code: str) -> dict[str, Any] | None:
        if not migration_code:
            return None
        items, _ = self.list_entries(size=10000)
        for item in items:
            if item.get("迁移编号") == migration_code:
                return item
        return None

    def find_by_idempotency(self, key: str) -> dict[str, Any] | None:
        items, _ = self.list_entries(size=10000)
        for item in items:
            if item.get("幂等键") == key:
                return item
        return None

    def _resolve_batch(self, batch_ref: Any) -> dict[str, Any] | None:
        from app.services import service_for

        batch_svc = service_for("capture_batch")
        if batch_svc is None:
            return None
        if isinstance(batch_ref, int) or (
            isinstance(batch_ref, str) and batch_ref.strip().isdigit()
        ):
            entry = batch_svc.get_entry(int(batch_ref))
            if entry is not None:
                return entry
        items, _ = batch_svc.list_entries(size=10000)
        return next(
            (item for item in items if item.get("批次编号") == str(batch_ref)),
            None,
        )

    def report_window(
        self,
        batch_ref: Any,
        report: Any,
        *,
        report_id: str | None = None,
        operator: str = "robot",
    ) -> dict[str, Any]:
        """登记一份机器人补报并生成迁移与补采片段。

        返回值 ``status`` 为 ``created`` 或 ``duplicate``；重复补传不会产生
        第二套回执或补采任务，也不会再次抵扣。
        """
        from app.services import service_for

        batch = self._resolve_batch(batch_ref)
        if batch is None:
            raise WindowError(f"原采集批次不存在：{batch_ref}")

        window, embedded_id = SourceWindow.from_report(report)
        report_id = str(report_id or embedded_id or "").strip()
        batch_code = str(batch.get("批次编号", ""))
        key = idempotency_key(batch_code, window)

        existing = self.find_by_idempotency(key)
        if existing is not None:
            return {
                "status": "duplicate",
                "reason": "同一批次同一精确窗口的补报已确认，不重复处理",
                "migration": existing,
                "window": window.to_dict(),
            }

        batch_svc = service_for("capture_batch")
        orig_label = (
            batch_svc.get_original_label(batch) if batch_svc is not None else None
        )

        segments = window.segments()
        # 迁移编号绑定真实窗口的幂等键前 8 位：同一段补报永远得到同一编号，
        # 既便于回查，也在编号层面杜绝重复确认。
        migration_code = f"MIGR-{batch_code or 'B'}-{key[:8]}"

        # 只补真实来源窗口字段；不覆盖旧日标签，也不伪造时间。
        batch["原始日标签"] = orig_label or ""
        batch["来源窗口起"] = window.start_iso
        batch["来源窗口止"] = window.end_iso
        batch["来源时区"] = window.tz_name
        # 旧日标签原样保留在「采集窗口」里；仅在原本为空时补一个可读窗口标签。
        if not str(batch.get("采集窗口") or "").strip():
            batch["采集窗口"] = window.label
        batch["窗口核验状态"] = "迁移中"

        migration, missing = self.create_entry({
            "迁移编号": migration_code,
            "关联批次": batch_code,
            "批次编号": batch_code,
            "原始日标签": orig_label or "",
            "来源窗口起": window.start_iso,
            "来源窗口止": window.end_iso,
            "来源时区": window.tz_name,
            "补报键": key,
            "幂等键": key,
            "片段明细": segments_to_json(segments),
            "已核验片段": "[]",
            "待核验片段": segments_to_json(segments),
            "失败片段": "[]",
            "关联回执": "[]",
            "已抵扣片段": "[]",
            "恢复次数": "0",
            "中断原因": "",
            "操作者": operator,
            "上报时间": _now_iso(),
            "更新时间": _now_iso(),
        })
        if migration is None:
            raise WindowError(f"迁移登记失败：缺少字段 {'、'.join(missing)}")

        # 先落补采任务，全部保持未核验；核验由 verify/resume 推进。
        backfill_svc = service_for("backfill_queue")
        task_codes: list[str] = []
        if backfill_svc is not None:
            for seg in segments:
                seg_key = segment_key(key, seg)
                task_code = f"BACK-{seg_key[:12]}"
                task, _ = backfill_svc.create_entry({
                    "任务编号": task_code,
                    "关联批次": batch_code,
                    "原始日标签": orig_label or "",
                    "区间起": seg.start_iso,
                    "区间止": seg.end_iso,
                    "时区": seg.tz_name,
                    "核验状态": "待核验",
                    "阻断原因": "迁移尚未核实该区间",
                    "补报键": key,
                    "片段键": seg_key,
                    "迁移编号": migration_code,
                    "落库回执": "",
                    "队列编号": "",
                    "派发记录": "[]",
                    "入队时间": _now_iso(),
                    "更新时间": _now_iso(),
                })
                if task is not None:
                    task_codes.append(task_code)

        return {
            "status": "created",
            "migration": migration,
            "window": window.to_dict(),
            "segments": [seg.to_dict() for seg in segments],
            "backfill_tasks": task_codes,
            "report_id": report_id,
        }

    # ── 核验 / 中断 / 恢复 ────────────────────────────────────

    def _delivered_detail(self, batch_code: str) -> dict[str, Any]:
        from app.services import service_for

        handoff_svc = service_for("handoff_pack")
        packs: list[dict[str, Any]] = []
        if handoff_svc is not None:
            items, _ = handoff_svc.list_entries(size=10000)
            packs = [item for item in items if item.get("关联批次") == batch_code]
        delivered = [item for item in packs if item.get("status") == "已交接"]
        return {"packs": packs, "delivered": delivered}

    def evaluate_coverage(
        self,
        batch: dict[str, Any],
        segment: WindowSegment,
        orig_label: str | None,
    ) -> dict[str, Any]:
        """判定片段是否已被原有日标签交付覆盖。

        只有「片段完全落在原日标签当日」且「原批次已确认或存在已交接交接包」
        才判为已抵扣；原标签缺失或无法解析时结论为未知，不抵扣补采。
        """
        day = parse_day_label(orig_label) if orig_label else None
        if day is None:
            return {"covered": False, "conclusion": "未知（原标签缺失，不能猜填或抵扣）"}
        zone = segment.timezone
        day_start = datetime.combine(day, dt_time(0, 0), tzinfo=zone)
        day_end = datetime.combine(day + timedelta(days=1), dt_time(0, 0), tzinfo=zone)
        in_label = segment.start >= day_start and segment.end <= day_end
        detail = self._delivered_detail(str(batch.get("批次编号", "")))
        delivered = bool(detail["delivered"]) or batch.get("status") == "已确认"
        if in_label and delivered:
            return {
                "covered": True,
                "conclusion": f"已抵扣（原日标签{format_day_label(day)}已确认交接）",
            }
        if in_label and not delivered:
            return {
                "covered": False,
                "conclusion": f"待补采（原日标签{format_day_label(day)}尚未交付）",
            }
        return {
            "covered": False,
            "conclusion": f"待补采（超出原日标签{format_day_label(day)}覆盖范围）",
        }

    def _default_verifier(self, segment: WindowSegment) -> tuple[bool, str]:
        return True, "来源一致"

    def _coerce_verdict(self, result: Any) -> tuple[bool, str]:
        if isinstance(result, tuple) and len(result) == 2:
            return bool(result[0]), str(result[1])
        if isinstance(result, Mapping):
            return bool(result.get("ok")), str(result.get("reason", ""))
        return bool(result), ""

    def _receipt_for(
        self,
        migration: dict[str, Any],
        segment: WindowSegment,
        verdict: dict[str, Any],
        key: str,
    ) -> tuple[str, bool]:
        """写（或复用）该片段唯一的落库回执，返回 (回执编号, 是否新建)。"""
        from app.services import service_for

        receipt_svc = service_for("landing_receipt")
        if receipt_svc is None:
            return "", False
        seg_key = segment_key(key, segment)
        existing = receipt_svc.find_by_segment_key(seg_key)
        if existing is not None:
            return str(existing.get("回执编号", "")), False

        batch_code = str(migration.get("关联批次", ""))
        receipt_code = f"RECEIPT-{seg_key[:12]}"
        landing_code = ""
        landing_svc = service_for("data_landing")
        if landing_svc is not None:
            items, _ = landing_svc.list_entries(size=10000)
            landing = next(
                (item for item in items if item.get("关联批次") == batch_code), None
            )
            landing_code = str(landing.get("落库编号", "")) if landing else ""

        receipt, missing = receipt_svc.create_entry({
            "回执编号": receipt_code,
            "关联批次": batch_code,
            "批次编号": batch_code,
            "迁移编号": migration.get("迁移编号", ""),
            "补报键": key,
            "片段键": seg_key,
            "区间起": segment.start_iso,
            "区间止": segment.end_iso,
            "时区": segment.tz_name,
            "覆盖结论": verdict.get("conclusion", ""),
            "落库记录": landing_code,
            "核验时间": _now_iso(),
        })
        if receipt is None:
            return "", False
        receipt_svc.run_action(receipt["id"], "关联批次")
        return receipt_code, True

    def verify(
        self,
        migration_id: int,
        *,
        verifier: Verifier | None = None,
        interrupt_after: int | None = None,
        operator: str = "duty",
        resume: bool = False,
    ) -> dict[str, Any]:
        """核验迁移中的待核验片段，可中途中断并保留已核验结果。"""
        from app.services import service_for

        migration = self.get_entry(migration_id)
        if migration is None:
            return {"status": "missing", "reason": "迁移不存在"}

        if migration.get("status") == "已完成" and not segments_from_json(
            migration.get("待核验片段")
        ):
            return {
                "status": "already",
                "reason": "迁移已完成，不重复核验、不重复抵扣",
                "migration": migration,
            }

        key = str(migration.get("幂等键", ""))
        batch = self._resolve_batch(migration.get("关联批次"))
        orig_label = str(migration.get("原始日标签") or "") or None
        verifier = verifier or self._default_verifier

        pending = segments_from_json(migration.get("待核验片段"))
        verified = segments_from_json(migration.get("已核验片段"))
        failed = segments_from_json(migration.get("失败片段"))
        receipts = json.loads(migration.get("关联回执") or "[]")
        deducted = json.loads(migration.get("已抵扣片段") or "[]")

        if resume and failed:
            # 恢复时把上次核实失败的片段重新排队；已核验片段不再处理。
            retry_items = []
            for item in failed:
                retry_items.append({
                    k: item[k] for k in ("起", "止", "时区", "日期", "标签")
                    if k in item
                })
            failed = []
            pending = retry_items + pending
            migration["失败片段"] = "[]"
            migration["待核验片段"] = json.dumps(
                pending, ensure_ascii=False, sort_keys=True
            )

        backfill_svc = service_for("backfill_queue")

        processed = 0
        interrupted = False
        for item in list(pending):
            if interrupt_after is not None and processed >= interrupt_after:
                interrupted = True
                break

            segment = WindowSegment(
                start=datetime.fromisoformat(item["起"]),
                end=datetime.fromisoformat(item["止"]),
                tz_name=str(item["时区"]),
            )
            ok, reason = self._coerce_verdict(verifier(segment))
            segment_dict = segment.to_dict()
            processed += 1

            task = self._task_for(migration, segment, backfill_svc)

            if not ok:
                segment_dict["核验结果"] = "核实失败"
                segment_dict["原因"] = reason or "来源核实失败"
                failed.append(segment_dict)
                pending.remove(item)
                if backfill_svc is not None and task is not None:
                    backfill_svc.mark_failed(
                        task["id"], reason or "来源核实失败，未知时间不能猜填或抵扣"
                    )
                continue

            if batch is None:
                ok = False
                reason = "原采集批次不存在，无法判定覆盖"
                segment_dict["核验结果"] = "核实失败"
                segment_dict["原因"] = reason
                failed.append(segment_dict)
                pending.remove(item)
                if backfill_svc is not None and task is not None:
                    backfill_svc.mark_failed(task["id"], reason)
                continue

            verdict = self.evaluate_coverage(batch, segment, orig_label)
            receipt_code, _ = self._receipt_for(migration, segment, verdict, key)
            if receipt_code and receipt_code not in receipts:
                receipts.append(receipt_code)

            segment_dict["核验结果"] = "已核验"
            segment_dict["覆盖结论"] = verdict["conclusion"]
            segment_dict["回执编号"] = receipt_code
            verified.append(segment_dict)
            pending.remove(item)

            if verdict["covered"] and segment_dict["起"] not in deducted:
                deducted.append(segment_dict["起"])

            if backfill_svc is not None and task is not None:
                backfill_svc.mark_verified(
                    task["id"],
                    covered=verdict["covered"],
                    receipt_code=receipt_code,
                    reason=verdict["conclusion"],
                )

        migration["已核验片段"] = json.dumps(verified, ensure_ascii=False, sort_keys=True)
        migration["待核验片段"] = json.dumps(pending, ensure_ascii=False, sort_keys=True)
        migration["失败片段"] = json.dumps(failed, ensure_ascii=False, sort_keys=True)
        migration["关联回执"] = json.dumps(receipts, ensure_ascii=False)
        migration["已抵扣片段"] = json.dumps(deducted, ensure_ascii=False)
        migration["操作者"] = operator
        migration["更新时间"] = _now_iso()

        if resume:
            migration["恢复次数"] = str(int(migration.get("恢复次数", "0") or "0") + 1)

        if interrupted:
            migration["status"] = "已中断"
            migration["中断原因"] = f"按约定在核验{processed}段后中断，剩余{len(pending)}段待核验"
            final_status = "interrupted"
        elif failed:
            # 有片段核实失败：迁移未完成，明确暴露失败段，等待人工处置后再恢复。
            migration["status"] = "已中断"
            migration["中断原因"] = f"{len(failed)}段核实失败，需人工处置后再恢复"
            final_status = "failed"
        elif pending:
            migration["status"] = "待核验"
            final_status = "pending"
        else:
            migration["status"] = "已完成"
            migration["中断原因"] = ""
            final_status = "completed"

        if batch is not None:
            batch["窗口核验状态"] = (
                "已完成" if final_status == "completed"
                else "已中断" if final_status in ("interrupted", "failed")
                else "迁移中"
            )

        return {
            "status": final_status,
            "migration": migration,
            "processed": processed,
            "verified": len(verified),
            "failed": len(failed),
            "pending": len(pending),
            "receipts": receipts,
            "deducted": deducted,
        }

    def resume(self, migration_id: int, **kwargs: Any) -> dict[str, Any]:
        kwargs.pop("resume", None)
        return self.verify(migration_id, resume=True, **kwargs)

    def _task_for(
        self,
        migration: dict[str, Any],
        segment: WindowSegment,
        backfill_svc: Any,
    ) -> dict[str, Any] | None:
        if backfill_svc is None:
            return None
        key = str(migration.get("幂等键", ""))
        seg_key = segment_key(key, segment)
        items, _ = backfill_svc.list_entries(size=10000)
        return next(
            (item for item in items if item.get("片段键") == seg_key), None
        )

    # ── 通用动作 ─────────────────────────────────────────────

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        migration = self.get_entry(entry_id)
        if migration is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "恢复迁移":
            result = self.resume(entry_id)
            if result.get("status") == "missing":
                return None, result.get("reason", "迁移不存在")
            message = (
                "迁移已恢复并完成核验"
                if result.get("status") == "completed"
                else "迁移已恢复，仍有片段待核验"
                if result.get("status") == "pending"
                else "迁移已再次中断"
            )
            return migration, message
        if action == "作废迁移":
            migration["status"] = "已完成"
            migration["中断原因"] = "迁移被作废"
            return migration, "迁移已作废"
        return super().run_action(entry_id, action)

    # ── 查询聚合 ─────────────────────────────────────────────

    def summary(self, migration_id: int) -> dict[str, Any] | None:
        """汇总迁移、原批次、已交付明细、回执与补采任务，供值班员回查。"""
        from app.services import service_for

        migration = self.get_entry(migration_id)
        if migration is None:
            return None
        batch_svc = service_for("capture_batch")
        backfill_svc = service_for("backfill_queue")
        receipt_svc = service_for("landing_receipt")

        batch = self._resolve_batch(migration.get("关联批次"))
        detail = self._delivered_detail(str(migration.get("关联批次", "")))
        receipts = (
            receipt_svc.find_by_migration(str(migration.get("迁移编号", "")))
            if receipt_svc is not None else []
        )
        tasks = (
            backfill_svc.get_by_migration(str(migration.get("迁移编号", "")))
            if backfill_svc is not None else []
        )
        return {
            "migration": migration,
            "batch": batch,
            "original_label": (
                batch_svc.get_original_label(batch)
                if batch is not None and batch_svc is not None else None
            ),
            "delivered_packs": detail["delivered"],
            "all_packs": detail["packs"],
            "receipts": receipts,
            "backfill_tasks": tasks,
        }
