"""来源窗口迁移场景测试：跨午夜补报、中断恢复、补采派发与历史兼容。

覆盖任务中的值班员场景：
* 旧日标签 ``[2026-11-21]`` 批次已确认交接；
* 机器人补报 23:30 → 次日 00:30 的真实来源窗口（Asia/Shanghai）；
* 迁移先核实午夜前一段后中断，午夜后一段核实失败；
* 从补采队列分别查询两段并尝试派发；
* 恢复迁移后再次查询/派发，首次被阻、后续处理、先前已交接记录可区分；
* 同一份补报不能重复抵扣、重复出回执或改写旧日标签。
"""
from __future__ import annotations

import json

import pytest

from app.pipeline import (
    dispatch_backfill_interval,
    migrate_source_window,
    query_backfill_queue,
    resume_source_window_migration,
    verify_source_window_migration,
)
from app.services import service_for
from app.source_window import (
    SourceWindow,
    WindowError,
    format_day_label,
    idempotency_key,
    parse_day_label,
)

LABEL = "[2026-11-21]"
BATCH = "CAPT-2026-11-21"
REPORT = {
    "窗口起": "2026-11-21T23:30:00+08:00",
    "窗口止": "2026-11-22T00:30:00+08:00",
    "时区": "Asia/Shanghai",
    "补报编号": "REP-202601",
}


def _fail_post_midnight(segment) -> tuple[bool, str]:
    if segment.start.astimezone(segment.timezone).date().isoformat() == "2026-11-22":
        return False, "午夜后来源核实失败"
    return True, "来源一致"


def _all_ok(segment) -> tuple[bool, str]:
    return True, "来源一致"


def _query(start: str, end: str):
    return query_backfill_queue(窗口起=start, 窗口止=end)


def _by_start(code: str, start: str):
    svc = service_for("backfill_queue")
    return next(
        item for item in svc.get_by_migration(code)
        if item.get("区间起") == start
    )


# ── 纯领域：窗口模型 ────────────────────────────────────────────


class TestSourceWindowModel:
    def test_day_label_is_not_a_timestamp(self):
        assert parse_day_label(LABEL).isoformat() == "2026-11-21"
        assert parse_day_label("[2026-11-21T23:30:00]") is None
        assert format_day_label(parse_day_label(LABEL)) == LABEL

    def test_cross_midnight_splits_in_source_timezone(self):
        window, _ = SourceWindow.from_report(REPORT)
        segments = window.segments()
        assert len(segments) == 2
        assert segments[0].start_iso == "2026-11-21T23:30:00+08:00"
        assert segments[0].end_iso == "2026-11-22T00:00:00+08:00"
        assert segments[1].start_iso == "2026-11-22T00:00:00+08:00"
        assert segments[1].end_iso == "2026-11-22T00:30:00+08:00"
        assert all(seg.tz_name == "Asia/Shanghai" for seg in segments)

    def test_midnight_split_uses_window_timezone_not_utc(self):
        # 同一真实区间（上海 23:30 → 次日 00:30 = UTC 15:30 → 16:30）：
        # 以 UTC 表述时不跨 UTC 午夜，只有一段；以上海表述时跨午夜两段。
        as_utc, _ = SourceWindow.from_report({
            "窗口起": "2026-11-21T15:30:00+00:00",
            "窗口止": "2026-11-21T16:30:00+00:00",
            "时区": "UTC",
        })
        as_shanghai, _ = SourceWindow.from_report(REPORT)
        assert len(as_utc.segments()) == 1
        assert len(as_shanghai.segments()) == 2
        assert as_utc.start == as_shanghai.start
        assert as_utc.end == as_shanghai.end

    def test_equivalent_timezone_spelling_confirms_once(self):
        first = migrate_source_window(BATCH, REPORT)
        second = migrate_source_window(BATCH, {
            "窗口起": "2026-11-21T15:30:00+00:00",
            "窗口止": "2026-11-21T16:30:00+00:00",
            "时区": "UTC",
        })
        assert first["status"] == "created"
        assert second["status"] == "duplicate"
        # 已登记的迁移仍保留机器人最初上报的时区。
        assert first["migration"]["来源时区"] == "Asia/Shanghai"

    def test_missing_timezone_is_rejected(self):
        with pytest.raises(WindowError):
            SourceWindow.from_report({
                "窗口起": "2026-11-21T23:30:00",
                "窗口止": "2026-11-22T00:30:00",
            })

    def test_missing_end_is_rejected(self):
        with pytest.raises(WindowError):
            SourceWindow.from_report({"窗口起": "2026-11-21T23:30:00+08:00"})

    def test_end_before_start_is_rejected(self):
        with pytest.raises(WindowError):
            SourceWindow.from_report({
                "窗口起": "2026-11-22T00:30:00+08:00",
                "窗口止": "2026-11-21T23:30:00+08:00",
                "时区": "Asia/Shanghai",
            })

    def test_idempotency_key_is_stable_and_window_specific(self):
        window, _ = SourceWindow.from_report(REPORT)
        other = SourceWindow.from_report("2026-11-21T22:00:00+08:00/2026-11-21T23:00:00+08:00")[0]
        assert idempotency_key(BATCH, window) == idempotency_key(BATCH, window)
        assert idempotency_key(BATCH, window) != idempotency_key(BATCH, other)
        assert idempotency_key(BATCH, window) != idempotency_key("OTHER", window)


# ── 迁移登记与兼容 ─────────────────────────────────────────────


class TestMigrationRegistration:
    def test_report_keeps_original_label_and_records_real_window(self):
        result = migrate_source_window(BATCH, REPORT)
        assert result["status"] == "created"
        batch_svc = service_for("capture_batch")
        batch = next(
            b for b in batch_svc.list_entries(size=1000)[0]
            if b["批次编号"] == BATCH
        )
        # 旧标签原样保留，不改成臆造时间戳。
        assert batch["采集窗口"] == LABEL
        assert batch["原始日标签"] == LABEL
        assert batch["来源窗口起"] == "2026-11-21T23:30:00+08:00"
        assert batch["来源窗口止"] == "2026-11-22T00:30:00+08:00"
        assert batch["来源时区"] == "Asia/Shanghai"

    def test_duplicate_report_confirms_only_once(self):
        first = migrate_source_window(BATCH, REPORT)
        second = migrate_source_window(BATCH, dict(REPORT))
        assert first["status"] == "created"
        assert second["status"] == "duplicate"
        assert second["migration"]["id"] == first["migration"]["id"]
        # 只有一套补采任务，没有因重复补传再生成一遍。
        code = first["migration"]["迁移编号"]
        tasks = service_for("backfill_queue").get_by_migration(code)
        assert len(tasks) == 2

    def test_no_timestamp_forged_when_report_lacks_time(self):
        before = service_for("capture_batch").get_entry(4)
        with pytest.raises(WindowError):
            migrate_source_window(BATCH, {"时区": "Asia/Shanghai"})
        after = service_for("capture_batch").get_entry(4)
        assert before["来源窗口起"] == after["来源窗口起"] == ""
        assert after["采集窗口"] == LABEL

    def test_history_batches_still_queryable(self):
        batch_svc = service_for("capture_batch")
        hits, total = batch_svc.list_entries(filters={"采集窗口": "2026-11-21"})
        assert total == 1
        assert hits[0]["批次编号"] == BATCH

    def test_history_handoff_lookupable_by_day_label(self):
        handoff_svc = service_for("handoff_pack")
        found = handoff_svc.find_by_day_label(LABEL)
        assert [p["交接编号"] for p in found] == ["HAND-2026-11-21"]
        # 通过通用查询条件也应命中。
        hits, total = handoff_svc.list_entries(filters={"原始日标签": LABEL})
        assert total == 1 and hits[0]["关联批次"] == BATCH

    def test_batch_window_query_accepts_date_bounds(self):
        migrate_source_window(BATCH, REPORT)
        batch_svc = service_for("capture_batch")
        # 只给日期时，窗口止=2026-11-21 覆盖当天剩余时间（含 23:30→24:00）。
        hits, total = batch_svc.list_entries(filters={
            "窗口起": "2026-11-21", "窗口止": "2026-11-21",
        })
        assert total == 1 and hits[0]["批次编号"] == BATCH

    def test_batch_without_window_not_returned_by_interval_query(self):
        batch_svc = service_for("capture_batch")
        hits, total = batch_svc.list_entries(filters={
            "窗口起": "2026-11-21T00:00:00+08:00",
            "窗口止": "2026-11-22T00:00:00+08:00",
        })
        assert total == 0  # 未迁移批次没有可核验区间，不猜测命中


# ── 中断、失败与恢复 ───────────────────────────────────────────


class TestInterruptedMigration:
    def _start(self):
        result = migrate_source_window(BATCH, REPORT)
        return result["migration"]["id"], result["migration"]["迁移编号"]

    def test_first_run_verifies_before_midnight_then_interrupts(self):
        mid, _ = self._start()
        result = verify_source_window_migration(
            mid, verifier=_fail_post_midnight, interrupt_after=1,
        )
        assert result["status"] == "interrupted"
        assert result["processed"] == 1
        assert result["verified"] == 1
        assert result["pending"] == 1
        assert result["failed"] == 0

    def test_interrupted_keeps_unverified_after_midnight_blocked(self):
        mid, code = self._start()
        verify_source_window_migration(mid, verifier=_fail_post_midnight, interrupt_after=1)
        after = _by_start(code, "2026-11-22T00:00:00+08:00")
        assert after["status"] == "待核验"
        assert after["核验状态"] == "待核验"
        # 证据：迁移中断后上游未给出核验结论，不能假定为已核实。
        before = _by_start(code, "2026-11-21T23:30:00+08:00")
        assert before["核验状态"] == "已核验"

    def test_later_verification_failure_blocks_segment(self):
        mid, code = self._start()
        verify_source_window_migration(mid, verifier=_fail_post_midnight, interrupt_after=1)
        result = resume_source_window_migration(mid, verifier=_fail_post_midnight)
        # 仍有片段核实失败：迁移不算完成，明确暴露失败段等待处置。
        assert result["status"] == "failed"
        assert result["failed"] == 1
        after = _by_start(code, "2026-11-22T00:00:00+08:00")
        assert after["status"] == "已阻断"
        assert after["核验状态"] == "核实失败"
        assert "核实失败" in after["阻断原因"]

    def test_resume_only_processes_remaining_segments(self):
        mid, code = self._start()
        verify_source_window_migration(mid, verifier=_fail_post_midnight, interrupt_after=1)
        receipt_svc = service_for("landing_receipt")
        before = {r["片段键"] for r in receipt_svc.find_by_migration(code)}
        result = resume_source_window_migration(mid, verifier=_all_ok)
        assert result["processed"] == 1  # 首段已核验，恢复只补其余
        after = {r["片段键"] for r in receipt_svc.find_by_migration(code)}
        assert before <= after
        assert len(after) == 2

    def test_reverify_completed_migration_is_noop(self):
        mid, code = self._start()
        verify_source_window_migration(mid, verifier=_all_ok, interrupt_after=1)
        resume_source_window_migration(mid, verifier=_all_ok)
        receipts = service_for("landing_receipt").find_by_migration(code)
        again = verify_source_window_migration(mid, verifier=_all_ok)
        assert again["status"] == "already"
        assert len(service_for("landing_receipt").find_by_migration(code)) == len(receipts)


# ── 端到端值班员场景 ───────────────────────────────────────────


class TestDutyScenario:
    def _arrange(self):
        """登记补报；首轮核实午夜前一段成功、午夜后一段失败，迁移随即中断。"""
        result = migrate_source_window(BATCH, REPORT)
        mid = result["migration"]["id"]
        code = result["migration"]["迁移编号"]
        first = verify_source_window_migration(mid, verifier=_fail_post_midnight)
        # 只有午夜前那段核实成功，整份迁移因午夜后失败而中断。
        assert first["status"] == "failed"
        assert first["verified"] == 1
        assert first["failed"] == 1
        return mid, code

    def test_query_two_ranges_before_resume(self):
        mid, code = self._arrange()
        pre = _query("2026-11-21T23:30:00+08:00", "2026-11-22T00:00:00+08:00")
        post = _query("2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00")
        assert [i["区间起"] for i in pre["items"]] == ["2026-11-21T23:30:00+08:00"]
        assert [i["区间起"] for i in post["items"]] == ["2026-11-22T00:00:00+08:00"]
        # 午夜前那段被原交付抵扣，不能派；午夜后那段核实失败，不能派。
        assert pre["dispatchable"] == []
        assert post["dispatchable"] == []
        statuses = {i["区间起"]: i["status"] for i in pre["items"] + post["items"]}
        assert statuses["2026-11-21T23:30:00+08:00"] == "已抵扣"
        assert statuses["2026-11-22T00:00:00+08:00"] == "已阻断"

    def test_first_blocked_dispatch_is_distinguishable(self):
        mid, code = self._arrange()
        pre = _query("2026-11-21T23:30:00+08:00", "2026-11-22T00:00:00+08:00")
        post = _query("2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00")
        covered = dispatch_backfill_interval(pre["items"][0]["id"])
        unverified = dispatch_backfill_interval(post["items"][0]["id"])
        assert covered["status"] == "blocked"
        assert covered["attempt_kind"] == "covered"
        assert unverified["status"] == "blocked"
        assert unverified["attempt_kind"] == "unverified"
        # 被阻的尝试都有留痕，但未生成任何补采派发。
        assert unverified["task"]["派发记录"] != "[]"
        queue_codes = [
            q["队列编号"] for q in service_for("task_queue").list_entries(size=1000)[0]
            if q["队列编号"].startswith("TASK-BACK")
        ]
        assert queue_codes == []

    def test_covered_segment_cannot_be_deducted_twice(self):
        mid, code = self._arrange()
        pre = _query("2026-11-21T23:30:00+08:00", "2026-11-22T00:00:00+08:00")
        dispatch_backfill_interval(pre["items"][0]["id"])
        # 恢复时即使是全成功，首段已核验也不会再抵扣/再出回执。
        resume_source_window_migration(mid, verifier=_all_ok)
        migration = service_for("window_migration").get_entry(mid)
        assert migration["已抵扣片段"] == json.dumps(
            ["2026-11-21T23:30:00+08:00"], ensure_ascii=False
        )
        again = dispatch_backfill_interval(pre["items"][0]["id"])
        assert again["status"] == "blocked"
        assert again["attempt_kind"] == "covered"

    def test_after_resume_post_midnight_dispatches_once(self):
        mid, code = self._arrange()
        post = _query("2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00")
        dispatch_backfill_interval(post["items"][0]["id"])  # 首次被阻
        resume_source_window_migration(mid, verifier=_all_ok)

        query_after = _query("2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00")
        assert [i["区间起"] for i in query_after["dispatchable"]] == [
            "2026-11-22T00:00:00+08:00"
        ]
        task_id = query_after["dispatchable"][0]["task_id"]
        first = dispatch_backfill_interval(task_id, node="ROBO-0002")
        second = dispatch_backfill_interval(task_id, node="ROBO-0002")
        assert first["status"] == "dispatched"
        assert second["status"] == "duplicate"
        # 同一区间只产出一条补采队列任务。
        backfill_tasks = service_for("backfill_queue").get_by_migration(code)
        assert len(backfill_tasks) == 2

    def test_recovered_migration_distinguishes_all_three_outcomes(self):
        """首次被阻、后续处理、先前已交接记录可在恢复后同屏区分。"""
        mid, code = self._arrange()
        pre = _query("2026-11-21T23:30:00+08:00", "2026-11-22T00:00:00+08:00")
        post = _query("2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00")
        first_block = dispatch_backfill_interval(post["items"][0]["id"])
        assert first_block["attempt_kind"] == "unverified"

        resume_source_window_migration(mid, verifier=_all_ok)
        task_id = _query(
            "2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00"
        )["dispatchable"][0]["task_id"]
        processed = dispatch_backfill_interval(task_id, node="ROBO-0002")
        assert processed["status"] == "dispatched"

        summary = service_for("window_migration").summary(mid)
        # 先前已交接：原交接包仍按旧标签可回查。
        delivered_ids = [p["交接编号"] for p in summary["delivered_packs"]]
        assert delivered_ids == ["HAND-2026-11-21"]
        assert summary["original_label"] == LABEL
        # 首次被阻与后续处理在同一任务的历史里可区分。
        history = json.loads(summary["backfill_tasks"][1]["派发记录"])
        assert history[0]["结果"] == "阻断"
        assert history[-1]["结果"] == "已派发"
        # 午夜前那段仍是抵扣，不重复补采。
        assert summary["backfill_tasks"][0]["status"] == "已抵扣"

    def test_receipts_link_verified_segments_and_timezone_to_batch(self):
        mid, code = self._arrange()
        resume_source_window_migration(mid, verifier=_all_ok)
        receipts = service_for("landing_receipt").find_by_migration(code)
        assert len(receipts) == 2
        assert {r["关联批次"] for r in receipts} == {BATCH}
        assert {r["时区"] for r in receipts} == {"Asia/Shanghai"}
        assert {r["区间起"] for r in receipts} == {
            "2026-11-21T23:30:00+08:00",
            "2026-11-22T00:00:00+08:00",
        }
        conclusions = {r["区间起"]: r["覆盖结论"] for r in receipts}
        assert "已抵扣" in conclusions["2026-11-21T23:30:00+08:00"]
        assert "待补采" in conclusions["2026-11-22T00:00:00+08:00"]

    def test_duplicate_report_after_resume_does_not_rededuct(self):
        mid, code = self._arrange()
        resume_source_window_migration(mid, verifier=_all_ok)
        before = service_for("window_migration").get_entry(mid)
        snapshot = (
            before["已抵扣片段"], before["关联回执"], len(
                service_for("landing_receipt").find_by_migration(code)
            ),
        )
        dup = migrate_source_window(BATCH, REPORT)
        assert dup["status"] == "duplicate"
        after = service_for("window_migration").get_entry(mid)
        assert (
            after["已抵扣片段"], after["关联回执"], len(
                service_for("landing_receipt").find_by_migration(code)
            ),
        ) == snapshot

    def test_failed_post_midnight_is_never_deducted(self):
        mid, code = self._arrange()
        # 午夜后段即便核实失败，也不能因为时间未知而被抵扣或补造。
        migration = service_for("window_migration").get_entry(mid)
        assert json.loads(migration["已抵扣片段"]) == ["2026-11-21T23:30:00+08:00"]
        failed = json.loads(migration["失败片段"])
        assert [f["起"] for f in failed] == ["2026-11-22T00:00:00+08:00"]
        # 失败段也没有落库回执。
        receipt_starts = {
            r["区间起"] for r in service_for("landing_receipt").find_by_migration(code)
        }
        assert "2026-11-22T00:00:00+08:00" not in receipt_starts

    def test_unverified_segment_not_dispatchable_even_if_time_looks_like_label(self):
        mid, code = self._arrange()
        # 午夜后段落在 00:00–00:30，不在原日标签当日；未核实前仍不可派。
        query = _query("2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00")
        assert query["dispatchable"] == []
        task = query["blocked"][0]
        assert task["核验状态"] == "核实失败"

    def test_dispatch_without_online_node_queues_without_repeating(self):
        mid, code = self._arrange()
        resume_source_window_migration(mid, verifier=_all_ok)
        for node in service_for("robot_node").list_entries(size=1000)[0]:
            node["status"] = "离线"
        task_id = _query(
            "2026-11-22T00:00:00+08:00", "2026-11-22T00:30:00+08:00"
        )["dispatchable"][0]["task_id"]

        first = dispatch_backfill_interval(task_id)
        second = dispatch_backfill_interval(task_id)
        assert first["status"] == "queued"
        assert second["status"] == "queued"
        # 两次尝试只复用同一条补采队列任务，不重复入队。
        queue_codes = [
            q["队列编号"] for q in service_for("task_queue").list_entries(size=1000)[0]
            if q["队列编号"] == f"TASK-BACK-{task_id}"
        ]
        assert len(queue_codes) == 1


# ── 兼容：历史查询与交接 ───────────────────────────────────────


class TestCompatibility:
    def test_batch_history_queries_work_after_migration(self):
        migrate_source_window(BATCH, REPORT)
        batch_svc = service_for("capture_batch")
        # 旧标签仍按标签能查。
        by_label, total = batch_svc.list_entries(filters={"采集窗口": "2026-11-21"})
        assert total == 1
        # 精确区间重叠也能查。
        by_window, total = batch_svc.list_entries(filters={
            "窗口起": "2026-11-21T23:45:00+08:00",
            "窗口止": "2026-11-21T23:50:00+08:00",
        })
        assert total == 1 and by_window[0]["批次编号"] == BATCH

    def test_handoff_records_lookupable_by_original_label_after_migration(self):
        migrate_source_window(BATCH, REPORT)
        handoff_svc = service_for("handoff_pack")
        found = handoff_svc.find_by_day_label(LABEL)
        assert found and all(p["status"] == "已交接" for p in found)

    def test_legacy_batch_with_text_window_is_untouched(self):
        # 种子里的 CAPT-0001 使用非日标签的占位窗口，迁移不应误改它。
        batch_svc = service_for("capture_batch")
        original = batch_svc.get_entry(1)["采集窗口"]
        migrate_source_window(BATCH, REPORT)
        assert batch_svc.get_entry(1)["采集窗口"] == original

    def test_pipeline_collect_can_carry_exact_window(self):
        from app.pipeline import collect_files_and_land_data

        result = collect_files_and_land_data(
            3, [{"文件名称": "windowed.csv", "文件类型": "text/csv", "字节大小": "1024"}],
            [], source_window={
                "窗口起": "2026-11-21T23:30:00+08:00",
                "窗口止": "2026-11-22T00:30:00+08:00",
                "时区": "Asia/Shanghai",
            },
        )
        assert result["status"] == "landed"
        batch = service_for("capture_batch").get_entry(result["batch_id"])
        assert batch["来源窗口起"] == "2026-11-21T23:30:00+08:00"
        assert batch["来源时区"] == "Asia/Shanghai"
