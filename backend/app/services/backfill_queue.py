"""补采队列服务：来源窗口迁移后的区间核对、派发与阻断记录。

迁移把机器人补报的精确区间切成片段后，每个片段进入补采队列。队列
不自行猜测时间，也不绕过迁移结果：

* 只有迁移明确核验过的片段才可能被派发；
* 落在原日标签覆盖范围内、且已交付的片段标为「已抵扣」，不重复补采；
* 核实失败的片段保持阻断，未核验的时间既不能猜填也不能抵扣补采；
* 每次派发尝试都写入派发记录与审计轨迹，首次被阻与后续处理可区分。

同一份补报（同一批次 + 同一精确窗口 + 同一时区）只确认一次，片段键
用于保证落库回执与派发不会重复。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.source_window import (
    SourceWindow,
    WindowError,
    parse_query_moment,
    window_overlaps,
)


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class BackfillQueueService(BaseModuleService):
    MODULE = "backfill_queue"
    ENTITY = "补采任务"
    OPERATION_TERM = "补采"
    LIST_FIELDS = [
        "任务编号", "关联批次", "原始日标签", "区间起", "区间止", "时区",
        "核验状态", "阻断原因", "补报键", "片段键", "迁移编号",
        "落库回执", "队列编号", "派发记录", "入队时间", "更新时间",
    ]
    REQUIRED_FIELDS = ["任务编号", "关联批次", "区间起", "区间止", "时区"]
    STATUS_ORDER = ["待核验", "待派发", "已阻断", "已派发", "已抵扣", "已完成"]
    ACTION_RULES = {"确认完成": "已完成", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []
    QUERY_FIELDS = ["窗口起", "窗口止", "批次编号", "迁移编号"]

    # ── 查询 ─────────────────────────────────────────────────

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """支持按精确区间重叠查询；时间未知的任务不会被区间查询命中。"""
        filters = dict(filters or {})
        query_start = filters.pop("窗口起", None) or filters.pop("区间起", "") or None
        query_end = filters.pop("窗口止", None) or filters.pop("区间止", "") or None
        batch_code = filters.pop("批次编号", None)
        if batch_code:
            filters["关联批次"] = batch_code

        rows, _ = super().list_entries(
            filters=filters, keyword=None, status=status, page=1, size=1_000_000
        )

        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if any(
                    needle in str(row.get(field, "")).casefold()
                    for field in ("任务编号", "关联批次", "原始日标签", "迁移编号")
                )
            ]

        if query_start or query_end:
            start_dt = parse_query_moment(query_start)
            end_dt = parse_query_moment(query_end, day_end=True)
            rows = [row for row in rows if self._row_overlaps(row, start_dt, end_dt)]

        total = len(rows)
        offset = (page - 1) * size
        return rows[offset:offset + size], total

    def get_window(self, task: int | dict[str, Any]) -> SourceWindow | None:
        entry = self.get_entry(task) if isinstance(task, int) else task
        if not entry:
            return None
        try:
            return SourceWindow.from_values(
                entry.get("区间起"), entry.get("区间止"), entry.get("时区")
            )
        except WindowError:
            return None

    def _row_overlaps(self, row: dict[str, Any], start, end) -> bool:
        window = self.get_window(row)
        if window is None:
            return False
        return window_overlaps(window.start, window.end, start, end)

    def get_by_migration(self, migration_code: str) -> list[dict[str, Any]]:
        items, _ = self.list_entries(size=10000)
        return [
            item for item in items
            if str(item.get("迁移编号", "")) == migration_code
        ]

    # ── 状态维护（供迁移编排调用）─────────────────────────────

    def mark_verified(
        self,
        task_id: int,
        *,
        covered: bool,
        receipt_code: str,
        reason: str = "",
    ) -> dict[str, Any] | None:
        entry = self.get_entry(task_id)
        if entry is None:
            return None
        entry["核验状态"] = "已核验"
        entry["status"] = "已抵扣" if covered else "待派发"
        entry["阻断原因"] = ""
        entry["落库回执"] = receipt_code
        entry["更新时间"] = _now_iso()
        return entry

    def mark_failed(self, task_id: int, reason: str) -> dict[str, Any] | None:
        entry = self.get_entry(task_id)
        if entry is None:
            return None
        entry["核验状态"] = "核实失败"
        entry["status"] = "已阻断"
        entry["阻断原因"] = reason
        entry["更新时间"] = _now_iso()
        return entry

    def mark_unverified(self, task_id: int, reason: str) -> dict[str, Any] | None:
        entry = self.get_entry(task_id)
        if entry is None:
            return None
        entry["核验状态"] = "待核验"
        entry["status"] = "已阻断"
        entry["阻断原因"] = reason
        entry["更新时间"] = _now_iso()
        return entry

    # ── 派发 ─────────────────────────────────────────────────

    def _append_attempt(
        self,
        entry: dict[str, Any],
        *,
        operator: str,
        outcome: str,
        reason: str,
        node: str = "",
        queue_code: str = "",
    ) -> None:
        try:
            history = json.loads(entry.get("派发记录") or "[]")
        except (TypeError, ValueError):
            history = []
        history.append({
            "时间": _now_iso(),
            "操作者": operator,
            "结果": outcome,
            "原因": reason,
            "节点": node,
            "队列编号": queue_code,
        })
        entry["派发记录"] = json.dumps(history, ensure_ascii=False)
        entry["更新时间"] = _now_iso()

    def _write_audit(
        self,
        entry: dict[str, Any],
        *,
        operator: str,
        outcome: str,
        reason: str,
    ) -> None:
        from app.services import service_for

        audit_svc = service_for("audit_trail")
        if audit_svc is None:
            return
        audit_code = f"AUDI-BACK-{entry.get('片段键', entry.get('id'))}"
        audit_svc.create_entry({
            "条目编号": audit_code,
            "操作对象": f"补采任务.{entry.get('任务编号', '')}",
            "操作类型": "补采派发",
            "操作者标识": operator,
            "操作时间": _now_iso(),
            "变更摘要": (
                f"区间[{entry.get('区间起')} → {entry.get('区间止')} "
                f"{entry.get('时区')}] 派发{outcome}：{reason}"
            ),
            "关联执行": "",
            "追溯标记": f"backfill-{entry.get('片段键', '')}",
        })

    def attempt_dispatch(
        self,
        task_id: int,
        *,
        operator: str = "duty",
        node: str = "",
    ) -> dict[str, Any]:
        """按迁移核验结果尝试派发补采区间。

        返回 ``ok`` 表示这次请求是否成功产生了新的派发；``status`` 会明确
        给出 ``blocked``（首次/再次被阻）、``dispatched``、``queued`` 或
        ``duplicate``。所有尝试都会留痕。
        """
        from app.services import service_for

        entry = self.get_entry(task_id)
        if entry is None:
            return {"ok": False, "status": "missing", "reason": "补采任务不存在"}

        if entry.get("status") == "已抵扣":
            reason = "区间已由原批次交付覆盖，不能重复补采，也不能重复抵扣"
            self._append_attempt(
                entry, operator=operator, outcome="阻断", reason=reason, node=node
            )
            self._write_audit(entry, operator=operator, outcome="阻断", reason=reason)
            return {
                "ok": False, "status": "blocked", "reason": reason,
                "task": entry, "attempt_kind": "covered",
            }

        if entry.get("status") == "已派发":
            reason = "该区间已派发，不重复派发"
            self._append_attempt(
                entry, operator=operator, outcome="重复", reason=reason, node=node
            )
            self._write_audit(entry, operator=operator, outcome="重复", reason=reason)
            return {
                "ok": False, "status": "duplicate", "reason": reason,
                "task": entry, "attempt_kind": "duplicate",
            }

        if entry.get("status") == "已完成":
            reason = "该区间补采已完成，不重复派发"
            self._append_attempt(
                entry, operator=operator, outcome="重复", reason=reason, node=node
            )
            return {
                "ok": False, "status": "duplicate", "reason": reason,
                "task": entry, "attempt_kind": "duplicate",
            }

        # 以迁移结果为准核对区间；不信任任务自身的孤立状态。
        migration_svc = service_for("window_migration")
        migration = None
        if migration_svc is not None:
            migration = migration_svc.find_by_code(str(entry.get("迁移编号") or ""))
        verified_starts = set()
        if migration is not None:
            try:
                verified = json.loads(migration.get("已核验片段") or "[]")
            except (TypeError, ValueError):
                verified = []
            verified_starts = {str(item.get("起", "")) for item in verified if isinstance(item, dict)}

        if entry.get("核验状态") != "已核验" or str(entry.get("区间起", "")) not in verified_starts:
            reason = (
                entry.get("阻断原因")
                or "区间未通过迁移核验，不能派发；未知时间不猜测也不抵扣补采"
            )
            entry["status"] = "已阻断"
            entry["阻断原因"] = reason
            self._append_attempt(
                entry, operator=operator, outcome="阻断", reason=reason, node=node
            )
            self._write_audit(entry, operator=operator, outcome="阻断", reason=reason)
            return {
                "ok": False, "status": "blocked", "reason": reason,
                "task": entry, "attempt_kind": "unverified",
            }

        # 核验通过且未被原批次覆盖：真正产生补采派发。
        batch_svc = service_for("capture_batch")
        batch = None
        if batch_svc is not None:
            batch_code = str(entry.get("关联批次", "")).strip()
            if batch_code.isdigit():
                candidate = batch_svc.get_entry(int(batch_code))
                batch = candidate if candidate is not None else None
            if batch is None:
                items, _ = batch_svc.list_entries(size=10000)
                batch = next(
                    (item for item in items
                     if item.get("批次编号") == batch_code),
                    None,
                )

        # 显式指定节点则沿用；否则按负载挑一个在线节点，没有就入队等待。
        target_node = node
        if not target_node:
            node_svc = service_for("robot_node")
            if node_svc is not None:
                nodes, _ = node_svc.list_entries(size=1000)
                online = [item for item in nodes if item.get("status") == "在线"]
                if online:
                    online.sort(key=lambda item: int(item.get("当前负载", "0") or "0"))
                    target_node = str(online[0].get("节点编号", ""))

        queue_svc = service_for("task_queue")
        queue_code = ""
        if queue_svc is not None:
            queue_code = str(entry.get("队列编号") or "") or f"TASK-BACK-{task_id}"
            existing_queue = next(
                (item for item in queue_svc.list_entries(size=10000)[0]
                 if item.get("队列编号") == queue_code),
                None,
            )
            if existing_queue is None:
                queue_values = {
                    "队列编号": queue_code,
                    "关联模板": (batch or {}).get("关联执行") or str(entry.get("关联批次", "")),
                    "优先级": "high",
                    "期望开始": entry.get("区间起", ""),
                    "重试次数": "0",
                    "占用节点": target_node,
                    "等待秒数": "0",
                    "入队时间": _now_iso(),
                }
                queue_entry, _ = queue_svc.create_entry(queue_values)
                if queue_entry is not None and target_node:
                    queue_svc.run_action(queue_entry["id"], "派发任务")
            elif target_node and existing_queue.get("status") == "待派发":
                existing_queue["占用节点"] = target_node
                queue_svc.run_action(existing_queue["id"], "派发任务")

        entry["队列编号"] = queue_code
        if target_node:
            entry["status"] = "已派发"
            entry["阻断原因"] = ""
            reason = f"区间已核验并派发到节点{target_node}"
            outcome = "已派发"
        else:
            entry["status"] = "待派发"
            reason = "区间已核验，但无在线节点，补采任务已入队等待"
            outcome = "入队等待"
        self._append_attempt(
            entry, operator=operator, outcome=outcome, reason=reason,
            node=target_node, queue_code=queue_code,
        )
        self._write_audit(entry, operator=operator, outcome=outcome, reason=reason)
        return {
            "ok": True,
            "status": "dispatched" if target_node else "queued",
            "reason": reason,
            "task": entry,
            "queue_code": queue_code,
            "node": target_node,
        }
