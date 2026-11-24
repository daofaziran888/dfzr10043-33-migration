"""交接包服务：批次打包、清单校验与下游交接。

把已确认批次连同字段映射、校验摘要和清单打包成交接包，
供下游数仓直接消费并回溯来源。退回后批次不得再次交接。

兼容旧日标签：交接包除 ``关联批次`` 外还保留 ``原始日标签``，
即使批次后来迁移成精确来源窗口、批次编号形态变化，历史交接回执
仍能按原 ``[2026-11-21]`` 标签查到已签收记录。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.source_window import parse_day_label


class HandoffPackService(BaseModuleService):
    MODULE = "handoff_pack"
    ENTITY = "交接包"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "交接编号", "关联批次", "目标系统", "清单文件数", "记录行数",
        "校验摘要", "交接版本", "交接时间", "原始日标签",
    ]
    REQUIRED_FIELDS = ["交接编号", "关联批次", "目标系统"]
    STATUS_ORDER = ["待打包", "打包中", "已交接", "交接退回"]
    ACTION_RULES = {"启动打包": "打包中", "确认交接": "已交接", "记录退回": "交接退回"}
    NEGATIVE_ACTIONS = []
    QUERY_FIELDS = ["原始日标签", "批次编号"]

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        filters = dict(filters or {})
        label_query = filters.pop("原始日标签", None)
        batch_query = filters.pop("批次编号", None)
        if batch_query:
            filters["关联批次"] = batch_query

        rows, _ = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=1, size=1_000_000
        )
        if label_query:
            day = parse_day_label(label_query)
            needle = label_query.strip()
            if day is not None:
                # 日标签查询同时匹配原始日标签和“关联批次恰为日标签”的历史形态。
                rows = [
                    row for row in rows
                    if parse_day_label(row.get("原始日标签")) == day
                    or (
                        not row.get("原始日标签")
                        and parse_day_label(row.get("关联批次")) == day
                    )
                ]
            else:
                rows = [
                    row for row in rows
                    if str(row.get("原始日标签", "")).strip() == needle
                ]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def reject_handoff(self, pack_id: int, reason: str) -> dict[str, Any]:
        """驳回交接包，退回后批次不得进入交接。"""
        entry = self.get_entry(pack_id)
        if entry is None:
            return {"ok": False, "reason": "交接包不存在"}
        self.run_action(pack_id, "记录退回")
        return {"ok": True, "pack_id": pack_id, "reason": reason, "action": "交接退回"}

    def get_delivered_packs(self, target_system: str | None = None) -> list[dict[str, Any]]:
        """获取已交接的包列表，可按目标系统筛选。"""
        items, _ = self.list_entries(size=10000)
        delivered = [it for it in items if it.get("status") == "已交接"]
        if target_system:
            delivered = [it for it in delivered if it.get("目标系统") == target_system]
        return delivered

    def find_by_day_label(
        self, day_label: str, *, delivered_only: bool = True
    ) -> list[dict[str, Any]]:
        """按旧日标签 ``[2026-11-21]`` 回查交接记录。

        匹配 ``原始日标签`` 字段；历史数据没有该字段时，退回到 ``关联批次``
        恰为日标签的形态。找不到就返回空，不猜测对应关系。
        """
        day = parse_day_label(day_label)
        if day is None:
            return []
        label = f"[{day.isoformat()}]"
        items, _ = self.list_entries(size=10000)
        matched = [
            item for item in items
            if parse_day_label(item.get("原始日标签")) == day
            or (not item.get("原始日标签") and parse_day_label(item.get("关联批次")) == day)
        ]
        if delivered_only:
            matched = [item for item in matched if item.get("status") == "已交接"]
        return matched