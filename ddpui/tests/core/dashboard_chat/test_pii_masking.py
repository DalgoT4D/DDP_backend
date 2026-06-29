from types import SimpleNamespace

from ddpui.core.dashboard_chat.orchestration import pii_masking


def _state_with_student_metadata():
    return {
        "org_id": 1,
        "metadata_artifact_payload": {
            "schema_version": 5,
            "dashboard_id": 1,
            "org_id": 1,
            "allowlisted_tables": ["dev.students"],
            "chart_table_map": {},
            "tables": [
                {
                    "table_name": "dev.students",
                    "schema_name": "dev",
                    "model_name": "students",
                    "columns": [
                        {
                            "column_name": "student_name",
                            "data_type": "text",
                            "pii": True,
                            "statistics": {"sample_values": ["Priya"]},
                        },
                        {
                            "column_name": "student_id",
                            "data_type": "text",
                            "pii": False,
                            "statistics": {"sample_values": ["student_123"]},
                        },
                    ],
                }
            ],
            "join_paths": [],
        },
    }


def _turn_context():
    return SimpleNamespace(
        pii_tokens_by_value={},
        pii_token_counters={},
        pii_value_map={},
    )


def _disable_pii_sharing(monkeypatch):
    monkeypatch.setattr(pii_masking, "share_pii_with_llms_enabled", lambda state: False)
    monkeypatch.setattr(pii_masking, "load_pii_overrides_for_org", lambda org: {})


def test_masks_direct_pii_result_column(monkeypatch):
    _disable_pii_sharing(monkeypatch)
    turn_context = _turn_context()

    masked_rows = pii_masking.mask_sql_rows_for_llm(
        state=_state_with_student_metadata(),
        turn_context=turn_context,
        sql="SELECT student_name, student_id FROM dev.students",
        rows=[{"student_name": "Priya", "student_id": "student_123"}],
    )

    assert masked_rows == [{"student_name": "[[PII_STUDENT_NAME_1]]", "student_id": "student_123"}]
    assert turn_context.pii_value_map == {"[[PII_STUDENT_NAME_1]]": "Priya"}


def test_masks_aliased_pii_result_column(monkeypatch):
    _disable_pii_sharing(monkeypatch)
    turn_context = _turn_context()

    masked_rows = pii_masking.mask_sql_rows_for_llm(
        state=_state_with_student_metadata(),
        turn_context=turn_context,
        sql="SELECT s.student_name AS learner FROM dev.students s",
        rows=[{"learner": "Akash"}],
    )

    assert masked_rows == [{"learner": "[[PII_LEARNER_1]]"}]
    assert (
        pii_masking.unmask_pii_text(
            "Top learner: [[PII_LEARNER_1]]",
            turn_context.pii_value_map,
        )
        == "Top learner: Akash"
    )


def test_does_not_mask_numeric_count_over_pii_column(monkeypatch):
    _disable_pii_sharing(monkeypatch)
    turn_context = _turn_context()

    masked_rows = pii_masking.mask_sql_rows_for_llm(
        state=_state_with_student_metadata(),
        turn_context=turn_context,
        sql="SELECT COUNT(DISTINCT student_name) AS student_count FROM dev.students",
        rows=[{"student_count": 42}],
    )

    assert masked_rows == [{"student_count": 42}]
    assert turn_context.pii_value_map == {}


def test_masks_metadata_sample_values_for_pii_columns(monkeypatch):
    _disable_pii_sharing(monkeypatch)
    turn_context = _turn_context()
    state = _state_with_student_metadata()
    artifact = pii_masking.load_effective_metadata_payload(state)

    masked_artifact = pii_masking.mask_metadata_artifact_for_llm(
        state=state,
        turn_context=turn_context,
        artifact=artifact,
    )

    columns = {column.column_name: column for column in masked_artifact.tables[0].columns}
    assert columns["student_name"].statistics.sample_values == ["[[PII_STUDENT_NAME_1]]"]
    assert columns["student_id"].statistics.sample_values == ["student_123"]
    assert turn_context.pii_value_map == {"[[PII_STUDENT_NAME_1]]": "Priya"}


def test_masks_answer_text_for_llm_reusable_history():
    masked_text = pii_masking.mask_text_with_pii_map(
        "Priya and Akash are below threshold.",
        {
            "[[PII_STUDENT_NAME_1]]": "Priya",
            "[[PII_STUDENT_NAME_2]]": "Akash",
        },
    )

    assert masked_text == ("[[PII_STUDENT_NAME_1]] and [[PII_STUDENT_NAME_2]] are below threshold.")
