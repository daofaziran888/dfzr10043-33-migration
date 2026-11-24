"""落库回执服务：把核实过的来源区间与其时区关联回原采集批次。

机器人补报的跨午夜窗口经迁移逐段核实后，每核实一段就写一张回执：
回执记录精确起止时间、来源时区、是否被原批次交付覆盖，以及与迁移、
原批次的关联。回执以片段键幂等，同一份补报不会重复落回执。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class LandingReceiptService(BaseModuleService):
    MODULE = "landing_receipt"
    ENTITY = "落库回执"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "回执编号", "关联批次", "批次编号", "迁移编号", "补报键", "片段键",
        "区间起", "区间止", "时区", "覆盖结论", "落库记录", "核验时间",
    ]
    REQUIRED_FIELDS = ["回执编号", "关联批次", "区间起", "区间止", "时区"]
    STATUS_ORDER = ["已出具", "已关联", "已作废"]
    ACTION_RULES = {"关联批次": "已关联", "作废回执": "已作废"}
    NEGATIVE_ACTIONS = ["作废回执"]

    def find_by_segment_key(self, key: str) -> dict[str, Any] | None:
        items, _ = self.list_entries(size=10000)
        for item in items:
            if item.get("片段键") == key:
                return item
        return None

    def find_by_batch(self, batch_code: str) -> list[dict[str, Any]]:
        items, _ = self.list_entries(size=10000)
        return [item for item in items if item.get("关联批次") == batch_code]

    def find_by_migration(self, migration_code: str) -> list[dict[str, Any]]:
        items, _ = self.list_entries(size=10000)
        return [item for item in items if item.get("迁移编号") == migration_code]
