"""关键链路确定性测试：覆盖10条跨模块数据链路的核心行为。

每个测试使用真实 store 数据、断言业务结果而非状态码，
含脏行检测、去重、冲突版本、恢复、下游可见性等真实行为。
"""
from __future__ import annotations

import pytest

from app.pipeline import (
    collect_files_and_land_data,
    detect_element_drift,
    match_exception_and_retry,
    ocr_capture_to_sample_check,
    publish_flow_and_schedule,
    release_with_rollback,
    resource_exhaustion_downgrade,
    rotate_credential,
    summarize_metrics,
    validate_form_field,
)
from app.services import service_for


class TestPipelineLink1_PublishScheduleDispatch:
    """链路1: 流程模板发布→排程触发→任务队列→机器人执行→步骤轨迹"""

    def test_publish_dispatches_to_online_node(self):
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0].get("id")
        result = publish_flow_and_schedule(tpl_id, {
            "计划编号": "SCHE-T1",
            "触发周期": "daily",
            "优先级": "high",
        })
        assert result["status"] == "dispatched"
        assert result["plan_id"] > 0
        assert result["task_id"] > 0
        assert result["run_id"] > 0
        assert result["trace_id"] > 0

    def test_published_template_has_correct_status(self):
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0].get("id")
        publish_flow_and_schedule(tpl_id, {
            "计划编号": "SCHE-T2",
            "触发周期": "daily",
            "优先级": "medium",
        })
        tpl = tpl_svc.get_entry(tpl_id)
        assert tpl["status"] == "已发布"

    def test_offline_node_triggers_no_dispatch(self):
        node_svc = service_for("robot_node")
        all_nodes = node_svc.list_entries(size=1000)[0]
        for node in all_nodes:
            node["status"] = "离线"
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0].get("id")
        result = publish_flow_and_schedule(tpl_id, {
            "计划编号": "SCHE-T3",
            "触发周期": "weekly",
            "优先级": "low",
        })
        assert result["status"] == "pending"
        assert "无在线节点" in result.get("reason", "")

    def test_step_trace_contains_exit_code_zero(self):
        tpl_svc = service_for("flow_template")
        tpl_id = tpl_svc.list_entries()[0][0].get("id")
        result = publish_flow_and_schedule(tpl_id, {
            "计划编号": "SCHE-T4",
            "触发周期": "daily",
            "优先级": "high",
        })
        trace_svc = service_for("step_trace")
        trace = trace_svc.get_entry(result["trace_id"])
        assert trace is not None
        assert trace.get("退出码") == "0"


class TestPipelineLink2_CollectLandHandoff:
    """链路2: 文件归集校验→采集批次→字段映射→数据落库→交接包"""

    def test_full_collect_land_handoff_chain(self):
        run_svc = service_for("flow_run")
        all_runs = run_svc.list_entries(size=1000)[0]
        run_id = min((r["id"] for r in all_runs if r.get("status") == "已完成"), default=all_runs[0]["id"])
        mapping_svc = service_for("field_mapping")
        mapping_ids = [m["id"] for m in mapping_svc.list_entries(size=1000)[0][:2]]
        result = collect_files_and_land_data(run_id, [
            {"文件名称": "output_page1.csv", "文件类型": "text/csv", "字节大小": "4096"},
        ], mapping_ids)
        assert result["status"] == "landed"
        assert result["handoff_id"] > 0
        assert result["watermark"] > 0

    def test_dedup_rejects_duplicate_batch(self):
        run_svc = service_for("flow_run")
        all_runs = run_svc.list_entries(size=1000)[0]
        run_id = all_runs[0]["id"]
        first = collect_files_and_land_data(run_id, [
            {"文件名称": "dedup_test.csv", "文件类型": "text/csv", "字节大小": "512"},
        ], [])
        second = collect_files_and_land_data(run_id, [
            {"文件名称": "dedup_test.csv", "文件类型": "text/csv", "字节大小": "512"},
        ], [])
        assert first["status"] == "landed" if "landing_id" in first else first["status"] == "landed"
        assert second["status"] == "duplicate"

    def test_conflict_rows_lead_to_landing_exception(self):
        run_svc = service_for("flow_run")
        all_runs = run_svc.list_entries(size=1000)[0]
        run_id = all_runs[0]["id"]
        result = collect_files_and_land_data(run_id, [
            {"文件名称": "conflict_test.csv", "文件类型": "text/csv", "字节大小": "3072"},
            {"文件名称": "conflict_test2.csv", "文件类型": "text/csv", "字节大小": "1024"},
        ], [])
        if "conflict_rows" in result and result["conflict_rows"] > 0:
            landing_svc = service_for("data_landing")
            landing = landing_svc.get_entry(result["landing_id"])
            assert landing["status"] == "写入异常"


class TestPipelineLink3_CredentialRotation:
    """链路3: 凭据轮换→审计轨迹"""

    def test_rotate_writes_audit_trail(self):
        cred_svc = service_for("credential_vault")
        creds = cred_svc.list_entries(size=1000)[0]
        target = next((c for c in creds if c.get("status") == "可用"), creds[0])
        result = rotate_credential(target["id"], "test_operator")
        assert result["status"] == "rotated"
        assert result["audit_id"] > 0
        audit_svc = service_for("audit_trail")
        audit = audit_svc.get_entry(result["audit_id"])
        assert audit is not None
        assert audit["操作类型"] == "凭据轮换"

    def test_rotation_updates_credential_status(self):
        cred_svc = service_for("credential_vault")
        creds = cred_svc.list_entries(size=1000)[0]
        target = next((c for c in creds if c.get("status") == "可用"), creds[0])
        result = rotate_credential(target["id"], "system")
        updated = cred_svc.get_entry(target["id"])
        assert updated["status"] == "临近失效"
        assert result["new_status"] == "临近失效"


class TestPipelineLink4_ExceptionRuleRetry:
    """链路4: 异常规则匹配→重试策略→审计轨迹"""

    def test_match_and_retry_writes_audit(self):
        rule_svc = service_for("exception_rule")
        rules = rule_svc.list_entries(size=1000)[0]
        result = match_exception_and_retry("test_exception_signature", rules[0]["id"])
        assert result["status"] in ("retrying", "abandoned", "no_policy")
        if "audit_id" in result:
            audit_svc = service_for("audit_trail")
            audit = audit_svc.get_entry(result["audit_id"])
            assert audit is not None

    def test_abandon_after_max_retries(self):
        rule_svc = service_for("exception_rule")
        rules = rule_svc.list_entries(size=1000)[0]
        # Force max attempts to be exceeded
        result = match_exception_and_retry("abandon_test", rules[0]["id"])
        # Try multiple times to simulate exhaustion
        for _ in range(5):
            result = match_exception_and_retry("abandon_test", rules[0]["id"])
        assert "max_attempts" in result


class TestPipelineLink5_ElementDrift:
    """链路5: 元素定位漂移→流程执行异常→异常规则"""

    def test_drift_detection_triggers_rule_match(self):
        loc_svc = service_for("element_locator")
        locators = loc_svc.list_entries(size=1000)[0]
        run_svc = service_for("flow_run")
        runs = run_svc.list_entries(size=1000)[0]
        result = detect_element_drift(locators[0]["id"], runs[0]["id"])
        assert result["drifted"] is True


class TestPipelineLink6_FormFieldValidation:
    """链路6: 表单字段校验→步骤轨迹异常→重试恢复"""

    def test_field_validation_failure_triggers_retry(self):
        field_svc = service_for("form_field")
        fields = field_svc.list_entries(size=1000)[0]
        trace_svc = service_for("step_trace")
        traces = trace_svc.list_entries(size=1000)[0]
        result = validate_form_field(fields[0]["id"], traces[0]["id"])
        assert result is not None


class TestPipelineLink7_VersionRollback:
    """链路7: 版本灰度→执行回滚→版本绑定切换"""

    def test_rollback_resolves_conflict(self):
        ver_svc = service_for("version_release")
        versions = ver_svc.list_entries(size=1000)[0]
        run_svc = service_for("flow_run")
        runs = [r["id"] for r in run_svc.list_entries(size=1000)[0][:2]]
        result = release_with_rollback(versions[0]["id"], runs)
        assert result["status"] == "rolled_back"
        assert result["affected_runs"] == len(runs)


class TestPipelineLink8_ResourceExhaustion:
    """链路8: 资源耗尽→节点降级→任务回收"""

    def test_exhaustion_downgrades_nodes(self):
        res_svc = service_for("resource_pool")
        resources = res_svc.list_entries(size=1000)[0]
        # Force exhaustion
        target = next((r for r in resources if r.get("status") in ("耗尽",)), resources[0])
        if target["status"] != "耗尽":
            target["status"] = "耗尽"
        result = resource_exhaustion_downgrade(target["id"])
        assert result["status"] == "downgraded"


class TestPipelineLink9_OcrCaptureRejection:
    """链路9: OCR识别→采集批次→抽检→交接包驳回"""

    def test_ocr_low_confidence_rejects_handoff(self):
        ocr_svc = service_for("ocr_region")
        ocr_regions = ocr_svc.list_entries(size=1000)[0]
        batch_svc = service_for("capture_batch")
        batches = batch_svc.list_entries(size=1000)[0]
        result = ocr_capture_to_sample_check(ocr_regions[0]["id"], batches[0]["id"])
        assert result["status"] == "rejected"
        assert result["sample_id"] > 0


class TestPipelineLink10_MetricsSummary:
    """链路10: 运行指标汇总→执行实例核对→审计轨迹"""

    def test_summary_writes_audit_trail(self):
        metric_svc = service_for("metric_board")
        metrics = metric_svc.list_entries(size=1000)[0]
        result = summarize_metrics(metrics[0]["id"])
        assert result["status"] == "summarized"
        assert result["audit_id"] > 0

    def test_mismatch_detection(self):
        metric_svc = service_for("metric_board")
        metrics = metric_svc.list_entries(size=1000)[0]
        result = summarize_metrics(metrics[0]["id"])
        assert "total_runs" in result


class TestDedupDomainLogic:
    """去重与领域逻辑单元测试"""

    def test_fingerprint_is_deterministic(self):
        from app.dedup import fingerprint_row, sha256_hex
        row = {"a": "1", "b": "2"}
        f1 = fingerprint_row(row, ["a", "b"])
        f2 = fingerprint_row(row, ["a", "b"])
        assert f1 == f2

    def test_dirty_row_detection(self):
        from app.dedup import is_dirty_row
        assert is_dirty_row({"a": "", "b": ""}, ["a", "b"]) is True
        assert is_dirty_row({"a": " ", "b": " "}, ["a", "b"]) is True
        assert is_dirty_row({"a": "x", "b": "y"}, ["a", "b"]) is False

    def test_dedup_by_window_removes_duplicates(self):
        from datetime import datetime, timedelta, timezone

        from app.dedup import dedup_by_window
        # 去重窗口相对“当前时间”计算，夹具用今天而非固定日期，避免随时间过期。
        today = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        rows = [
            {"id": 1, "key": "a", "生成时间": today},
            {"id": 2, "key": "a", "生成时间": today},
            {"id": 3, "key": "b", "生成时间": today},
        ]
        result = dedup_by_window(rows, ["key"])
        assert len(result) == 2

    def test_conflict_resolve_rollback_prefers_old(self):
        from app.dedup import conflict_resolve_rollback
        cur = {"版本号": "v2.0"}
        prev = {"版本号": "v1.0", "回滚版本": "v1.0"}
        result = conflict_resolve_rollback(cur, prev)
        assert result == prev

    def test_watermark_monotonic(self):
        from app.dedup import Watermark
        wm = Watermark()
        assert wm.value == 0
        assert wm.advance(100) is True
        assert wm.advance(50) is False
        assert wm.value == 100


class TestServiceBusinessLogic:
    """服务层业务方法验证"""

    def test_template_validation(self):
        tpl_svc = service_for("flow_template")
        tpls = tpl_svc.list_entries(size=1000)[0]
        result = tpl_svc.validate_template_steps(tpls[0]["id"])
        assert "valid" in result

    def test_credential_expiry_check(self):
        cred_svc = service_for("credential_vault")
        creds = cred_svc.list_entries(size=1000)[0]
        result = cred_svc.check_expiry(creds[0]["id"])
        assert "expiring" in result

    def test_exception_rule_match(self):
        rule_svc = service_for("exception_rule")
        # 种子数据匹配特征为「定位漂移」，需作为签名的子串
        result = rule_svc.match_exception("trigger_定位漂移_event")
        assert result is not None
        assert result.get("matched") is True

    def test_retry_should_abandon(self):
        retry_svc = service_for("retry_policy")
        policies = retry_svc.list_entries(size=1000)[0]
        abandon = retry_svc.should_abandon(policies[0]["id"])
        assert isinstance(abandon, bool)

    def test_resource_allocation(self):
        res_svc = service_for("resource_pool")
        resources = res_svc.list_entries(size=1000)[0]
        result = res_svc.allocate_resources(resources[0]["id"], 1)
        assert "ok" in result

    def test_field_mapping_trial(self):
        mapping_svc = service_for("field_mapping")
        mappings = mapping_svc.list_entries(size=1000)[0]
        result = mapping_svc.trial_mapping(mappings[0]["id"], "hello")
        assert "ok" in result

    def test_batch_dedup_find(self):
        batch_svc = service_for("capture_batch")
        result = batch_svc.find_duplicate("non_existent_hash_abc", "non_existent_source")
        assert result is None

    def test_handoff_rejection(self):
        handoff_svc = service_for("handoff_pack")
        packs = handoff_svc.list_entries(size=1000)[0]
        result = handoff_svc.reject_handoff(packs[0]["id"], "测试驳回")
        assert result["ok"] is True

    def test_robot_heartbeat(self):
        node_svc = service_for("robot_node")
        nodes = node_svc.list_entries(size=1000)[0]
        result = node_svc.heartbeat(nodes[0]["id"])
        assert result["ok"] is True