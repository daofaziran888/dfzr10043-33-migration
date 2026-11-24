"""采集批次服务：执行产出归集、完整性校验、窗口去重和批次确认。

一次或多次流程执行的产出记录归集成批次，附带来源窗口和完整性校验值
供下游引用。去重逻辑按来源窗口+上游来源判重。

兼容性约定：
* 历史批次的 ``采集窗口`` 仍是 ``[2026-11-21]`` 日标签，不做改写；
* 精确窗口只来自机器人补报，落在 ``来源窗口起/止/时区`` 字段；
* 时间未知的批次不参与区间查询，也不猜测其起止时间。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.source_window import (
    SourceWindow,
    WindowError,
    is_day_label,
    parse_day_label,
    parse_query_moment,
    window_overlaps,
)


class CaptureBatchService(BaseModuleService):
    MODULE = "capture_batch"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "批次编号", "关联执行", "记录行数", "采集窗口", "原始日标签",
        "来源窗口起", "来源窗口止", "来源时区", "窗口核验状态",
        "已核验片段", "完整性校验", "批次标签", "上游来源", "生成时间",
    ]
    REQUIRED_FIELDS = ["批次编号", "关联执行", "记录行数"]
    STATUS_ORDER = ["归集中", "校验中", "已确认", "校验失败"]
    ACTION_RULES = {"启动校验": "校验中", "确认批次": "已确认", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = []

    # 区间查询使用虚拟字段，不落库。
    QUERY_FIELDS = ["窗口起", "窗口止", "窗口时间"]

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """在通用查询之上支持按时间区间重叠查询。

        ``窗口起``/``窗口止`` 是查询边界（左闭右开）；历史日标签批次只有
        标签、没有真实时间，因此不会被区间查询命中，也不会被猜测时间。
        """
        filters = dict(filters or {})
        query_start = filters.pop("窗口起", None) or filters.pop("窗口时间", "") or None
        query_end = filters.pop("窗口止", None) or None

        rows, _ = super().list_entries(
            filters=filters, keyword=keyword, status=status, page=1, size=1_000_000
        )

        if query_start or query_end:
            start_dt = parse_query_moment(query_start)
            end_dt = parse_query_moment(query_end, day_end=True)
            rows = [
                row for row in rows
                if self._row_overlaps(row, start_dt, end_dt)
            ]

        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    # ── 窗口相关 ─────────────────────────────────────────────

    def get_window(self, batch: int | dict[str, Any]) -> SourceWindow | None:
        """读取批次的精确来源窗口；历史日标签批次返回 ``None``，不猜测。"""
        entry = self.get_entry(batch) if isinstance(batch, int) else batch
        if not entry:
            return None
        start = entry.get("来源窗口起")
        end = entry.get("来源窗口止")
        tz_name = entry.get("来源时区")
        if not start or not end or not tz_name:
            return None
        try:
            return SourceWindow.from_values(start, end, tz_name)
        except WindowError:
            return None

    def get_original_label(self, batch: int | dict[str, Any]) -> str | None:
        """返回批次最初的日标签；只回查原始标签，不反推时间。"""
        entry = self.get_entry(batch) if isinstance(batch, int) else batch
        if not entry:
            return None
        label = str(entry.get("原始日标签") or "").strip()
        if label:
            return label
        window_label = str(entry.get("采集窗口") or "").strip()
        if is_day_label(window_label):
            return window_label
        return None

    def get_original_day(self, batch: int | dict[str, Any]) -> str | None:
        label = self.get_original_label(batch)
        day = parse_day_label(label) if label else None
        return day.isoformat() if day else None

    def _row_overlaps(
        self, row: dict[str, Any], start, end
    ) -> bool:
        window = self.get_window(row)
        if window is None:
            return False
        return window_overlaps(window.start, window.end, start, end)

    def find_duplicate(self, checksum: str, source: str) -> dict[str, Any] | None:
        """按完整性校验值和上游来源查找重复批次。"""
        items, _ = self.list_entries(size=10000)
        for it in items:
            if it.get("完整性校验") == checksum and it.get("上游来源") == source:
                return it
        return None

    def get_confirmed_batches(self) -> list[dict[str, Any]]:
        """获取所有已确认的采集批次，供下游交接打包。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("status") == "已确认"]

    def verify_integrity(self, batch_id: int) -> dict[str, Any]:
        """启动校验并返回校验结果。"""
        entry = self.get_entry(batch_id)
        if entry is None:
            return {"ok": False, "reason": "批次不存在"}
        self.run_action(batch_id, "启动校验")
        row_count = int(entry.get("记录行数", "0") or "0")
        if row_count <= 0:
            self.run_action(batch_id, "标记失败")
            return {"ok": False, "reason": "批次记录行数为0，校验失败"}
        self.run_action(batch_id, "确认批次")
        return {"ok": True, "batch_id": batch_id, "row_count": row_count}
