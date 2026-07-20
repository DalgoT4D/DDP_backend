from unittest.mock import patch, MagicMock

import pytest

from ddpui.core.trial import warehouse_data


@patch("ddpui.core.trial.warehouse_data.subprocess")
def test_copy_runs_dump_then_restore(mock_subprocess):
    mock_subprocess.run.return_value = MagicMock(returncode=0, stderr="")
    src = {"host": "sh", "port": 5432, "database": "sdb", "username": "su", "password": "sp"}
    dst = {"host": "dh", "port": 5432, "database": "ddb", "username": "du", "password": "dp"}

    warehouse_data.copy_warehouse_data(src, dst, "/tmp/dump.pgc")

    assert mock_subprocess.run.call_count == 2
    dump_cmd = mock_subprocess.run.call_args_list[0].args[0]
    restore_cmd = mock_subprocess.run.call_args_list[1].args[0]
    # binary is configurable (may be an absolute path via TRIALS_PG_DUMP_BIN); match by name
    assert dump_cmd[0].endswith("pg_dump")
    assert "sdb" in dump_cmd
    assert restore_cmd[0].endswith("pg_restore")
    assert "ddb" in restore_cmd
    # PGPASSWORD passed via env, not argv
    dump_env = mock_subprocess.run.call_args_list[0].kwargs["env"]
    assert dump_env["PGPASSWORD"] == "sp"


@patch("ddpui.core.trial.warehouse_data.subprocess")
def test_copy_raises_on_dump_failure(mock_subprocess):
    mock_subprocess.run.return_value = MagicMock(returncode=1, stderr="boom")
    src = {"host": "sh", "port": 5432, "database": "sdb", "username": "su", "password": "sp"}
    dst = {"host": "dh", "port": 5432, "database": "ddb", "username": "du", "password": "dp"}

    with pytest.raises(RuntimeError, match="pg_dump failed"):
        warehouse_data.copy_warehouse_data(src, dst, "/tmp/dump.pgc")
