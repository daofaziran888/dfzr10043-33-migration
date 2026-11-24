"""来源窗口领域模型：精确起止时间 + 时区，替代只有日粒度的旧标签。

采集流水线历史上用 ``[2026-11-21]`` 这样的日标签表示采集范围。日标签
无法回答"这一批到底覆盖几点到几点"，跨午夜时尤其含糊。本模块提供：

* :class:`SourceWindow` —— 带时区的精确来源窗口，保留机器人上报的时区；
* :class:`WindowSegment` —— 按来源时区本地午夜切开的区间片段；
* 日标签解析 —— 旧标签只当作标签，**绝不**据此推断/补造具体时间戳；
* 幂等键计算 —— 同一份补报重复上传时能够识别为同一次确认。

这里只放纯领域逻辑，不读写数据仓库。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Any, Iterable, Mapping

DAY_LABEL_RE = re.compile(r"^\[?(\d{4}-\d{2}-\d{2})\]?$")
_OFFSET_RE = re.compile(r"^(?:UTC)?([+-])(\d{2}):?(\d{2})$", re.IGNORECASE)

# 兼容旧系统/前端常见的时间分隔符
_INTERVAL_SEPARATORS = ("/", "~", "→", " - ", " 至 ")

DEFAULT_TZ = "Asia/Shanghai"


class WindowError(ValueError):
    """来源窗口无法确定或不合法。调用方应拒绝处理，而不是猜填。"""


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        raise WindowError("来源窗口缺少时间，不能猜填")
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise WindowError(f"来源窗口时间格式不合法：{value}") from exc


def resolve_timezone(name: str) -> tzinfo:
    """解析 IANA 时区名或 ``UTC+08:00`` 形式的固定偏移。"""
    text = (name or "").strip()
    if not text:
        raise WindowError("来源窗口缺少时区，不能猜填")
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError):
        match = _OFFSET_RE.match(text)
        if not match:
            raise WindowError(f"无法识别的时区：{name}") from None
        sign = 1 if match.group(1) == "+" else -1
        hours = int(match.group(2))
        minutes = int(match.group(3))
        return timezone(sign * timedelta(hours=hours, minutes=minutes), text)


def offset_timezone_name(value: datetime) -> str:
    """把带偏移的时间转成稳定的 ``UTC+08:00`` 名称。"""
    offset = value.utcoffset()
    if offset is None:
        raise WindowError("来源窗口时间必须携带时区")
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    return f"UTC{sign}{total // 3600:02d}:{(total % 3600) // 60:02d}"


def parse_day_label(value: Any) -> date | None:
    """解析旧日标签 ``[2026-11-21]``。

    只返回"标签上的日期"，不附带任何钟点。返回 ``None`` 表示不是日标签。
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    match = DAY_LABEL_RE.match(str(value or "").strip())
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def is_day_label(value: Any) -> bool:
    return parse_day_label(value) is not None


def format_day_label(day: date) -> str:
    return f"[{day.isoformat()}]"


@dataclass(frozen=True)
class WindowSegment:
    """来源窗口在来源时区本地午夜处切出的片段。"""

    start: datetime
    end: datetime
    tz_name: str

    @property
    def timezone(self) -> tzinfo:
        return resolve_timezone(self.tz_name)

    @property
    def local_start(self) -> datetime:
        return self.start.astimezone(self.timezone)

    @property
    def day(self) -> str:
        """片段起始日在来源时区中的日期，例如 ``2026-11-21``。"""
        return self.local_start.date().isoformat()

    @property
    def start_iso(self) -> str:
        return self.start.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end.isoformat()

    @property
    def label(self) -> str:
        return f"[{self.start_iso} → {self.end_iso} {self.tz_name}]"

    def to_dict(self) -> dict[str, Any]:
        return {
            "起": self.start_iso,
            "止": self.end_iso,
            "时区": self.tz_name,
            "日期": self.day,
            "标签": self.label,
        }


@dataclass(frozen=True)
class SourceWindow:
    """带时区的精确来源窗口。"""

    start: datetime
    end: datetime
    tz_name: str

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise WindowError("来源窗口必须携带时区，不能猜填")
        if self.end <= self.start:
            raise WindowError("来源窗口的结束时间必须晚于开始时间")
        zone = resolve_timezone(self.tz_name)
        # 上报的偏移必须与声明的时区一致；不一致时明确拒绝，避免静默改写。
        if self.start.astimezone(zone).utcoffset() != self.start.utcoffset():
            raise WindowError(f"窗口起始时间与声明时区 {self.tz_name} 不一致")
        if self.end.astimezone(zone).utcoffset() != self.end.utcoffset():
            raise WindowError(f"窗口结束时间与声明时区 {self.tz_name} 不一致")

    # ── 构造 ─────────────────────────────────────────────────

    @classmethod
    def from_report(cls, report: Any) -> tuple["SourceWindow", str | None]:
        """从机器人补报解析来源窗口。

        支持两种写法：

        * 映射：``{"窗口起": ..., "窗口止": ..., "时区": ...}``
        * 字符串：``"2026-11-21T23:30:00+08:00/2026-11-22T00:30:00+08:00"``

        缺少任一时间或时区时抛 :class:`WindowError`，绝不补默认值。
        返回 ``(window, 补报编号)``。
        """
        report_id: str | None = None
        tz_name: str | None = None
        if isinstance(report, Mapping):
            start_raw = (
                report.get("窗口起")
                or report.get("来源窗口起")
                or report.get("start")
                or report.get("开始时间")
            )
            end_raw = (
                report.get("窗口止")
                or report.get("来源窗口止")
                or report.get("end")
                or report.get("结束时间")
            )
            tz_name = (
                report.get("时区")
                or report.get("来源时区")
                or report.get("timezone")
                or report.get("tz")
            )
            report_id = (
                report.get("补报编号")
                or report.get("report_id")
                or report.get("id")
            )
        elif isinstance(report, str):
            text = report.strip()
            for separator in _INTERVAL_SEPARATORS:
                if separator in text:
                    start_raw, end_raw = (part.strip() for part in text.split(separator, 1))
                    break
            else:
                raise WindowError("来源窗口字符串必须是“起始/结束”区间")
        else:
            raise WindowError("无法识别的来源窗口格式")

        if not start_raw or not end_raw:
            raise WindowError("来源窗口缺少起始或结束时间，不能猜填")

        start = _parse_datetime(start_raw)
        end = _parse_datetime(end_raw)

        if tz_name:
            zone = resolve_timezone(str(tz_name))
            if start.tzinfo is None:
                start = start.replace(tzinfo=zone)
            else:
                start = start.astimezone(zone)
            if end.tzinfo is None:
                end = end.replace(tzinfo=zone)
            else:
                end = end.astimezone(zone)
            tz_name = str(tz_name).strip()
        else:
            if start.tzinfo is None or end.tzinfo is None:
                raise WindowError("来源窗口缺少时区，不能猜填")
            if start.utcoffset() != end.utcoffset():
                raise WindowError("跨时区的来源窗口必须显式声明时区")
            tz_name = offset_timezone_name(start)

        return cls(start=start, end=end, tz_name=tz_name), (
            str(report_id).strip() if report_id else None
        )

    @classmethod
    def from_values(cls, start: Any, end: Any, tz_name: Any) -> "SourceWindow":
        """从已落库的窗口起止/时区重建窗口。"""
        start_dt = _parse_datetime(start)
        end_dt = _parse_datetime(end)
        zone = resolve_timezone(str(tz_name or ""))
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=zone)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=zone)
        return cls(start=start_dt, end=end_dt, tz_name=str(tz_name).strip())

    # ── 展示与切分 ───────────────────────────────────────────

    @property
    def timezone(self) -> tzinfo:
        return resolve_timezone(self.tz_name)

    @property
    def start_iso(self) -> str:
        return self.start.isoformat()

    @property
    def end_iso(self) -> str:
        return self.end.isoformat()

    @property
    def label(self) -> str:
        return f"[{self.start_iso} → {self.end_iso} {self.tz_name}]"

    def to_dict(self) -> dict[str, Any]:
        return {
            "起": self.start_iso,
            "止": self.end_iso,
            "时区": self.tz_name,
            "标签": self.label,
        }

    def segments(self) -> list[WindowSegment]:
        """按来源时区的本地午夜切分，跨午夜窗口会自然得到多段。"""
        zone = self.timezone
        cursor = self.start.astimezone(zone)
        end_local = self.end.astimezone(zone)
        result: list[WindowSegment] = []
        while cursor < end_local:
            next_midnight = datetime.combine(
                cursor.date() + timedelta(days=1), time(0, 0), tzinfo=zone
            )
            segment_end = min(next_midnight, end_local)
            result.append(WindowSegment(cursor, segment_end, self.tz_name))
            cursor = segment_end
        return result

    def contains(self, moment: datetime) -> bool:
        return self.start <= moment < self.end


def idempotency_key(batch_code: str, window: SourceWindow) -> str:
    """同一批次 + 同一真实区间的稳定幂等键。

    用 UTC 瞬时而非时区名称参与计算：同一段真实时间用 ``Asia/Shanghai``
    或等价的 ``UTC+08:00`` 补传两次，仍是同一次确认；存储时区不受影响。
    """
    payload = "|".join(
        [
            str(batch_code or ""),
            window.start.astimezone(timezone.utc).isoformat(),
            window.end.astimezone(timezone.utc).isoformat(),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def segment_key(report_key: str, segment: WindowSegment) -> str:
    """同一份补报下，每个片段独立的幂等键（用于落库回执/补采任务去重）。"""
    payload = f"{report_key}|{segment.start_iso}|{segment.end_iso}|{segment.tz_name}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def segments_to_json(segments: Iterable[WindowSegment]) -> str:
    return json.dumps(
        [segment.to_dict() for segment in segments],
        ensure_ascii=False,
        sort_keys=True,
    )


def segments_from_json(text: Any) -> list[dict[str, Any]]:
    if not text:
        return []
    if isinstance(text, list):
        return [dict(item) for item in text]
    try:
        loaded = json.loads(str(text))
    except (TypeError, ValueError):
        return []
    if not isinstance(loaded, list):
        return []
    return [dict(item) for item in loaded if isinstance(item, Mapping)]


def parse_query_moment(
    value: Any, default_tz: str = DEFAULT_TZ, *, day_end: bool = False
) -> datetime | None:
    """解析查询用的时间。

    只有日期时，``day_end=False`` 解释为当日零点（区间起点），
    ``day_end=True`` 解释为次日零点（左闭右开区间的上界），
    这样 ``窗口止=2026-11-21`` 能覆盖当天。
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        day = parse_day_label(text)
        if day is not None:
            if day_end:
                day = day + timedelta(days=1)
            return datetime.combine(day, time(0, 0), tzinfo=resolve_timezone(default_tz))
        parsed = _parse_datetime(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=resolve_timezone(default_tz))
    return parsed


def window_overlaps(
    window_start: datetime | None,
    window_end: datetime | None,
    query_start: datetime | None,
    query_end: datetime | None,
) -> bool:
    """半开区间重叠判断；任一侧时间未知则不认为重叠（不猜填）。"""
    if query_start is None and query_end is None:
        return True
    if window_start is None or window_end is None:
        return False
    if query_start is not None and window_end <= query_start:
        return False
    if query_end is not None and window_start >= query_end:
        return False
    return True
