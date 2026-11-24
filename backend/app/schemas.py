"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class WindowReportPayload(BaseModel):
    """机器人补报：精确起止必须由上报方给出，缺任一字段都应被拒绝。"""

    values: dict[str, Any] = Field(default_factory=dict)
    operator: str = "robot"
    verify: bool = False
    interrupt_after: int | None = None
    # 片段核验结论：``{"2026-11-22T00:00:00+08:00": {"ok": false, "reason": "..."}}``
    # 由机器人/来源核对给出；缺省片段视为来源一致。
    verdicts: dict[str, Any] = Field(default_factory=dict)


class DispatchPayload(BaseModel):
    """补采派发请求；节点为空时按在线节点负载选择。"""

    node: str = ""
    operator: str = "duty"
