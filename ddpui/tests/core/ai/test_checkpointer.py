"""Tests for checkpointer connection-string building (pure logic only —
pool/saver lifecycle is exercised by the REPL and consumer integration)."""

from ddpui.core.ai.agent.checkpointer import build_conninfo


def test_build_conninfo_from_django_db_settings():
    conninfo = build_conninfo(
        {
            "NAME": "dalgo",
            "HOST": "localhost",
            "PORT": "5432",
            "USER": "postgres",
            "PASSWORD": "p@ss word",
        }
    )
    assert conninfo.startswith("postgresql://")
    assert "p%40ss%20word" in conninfo  # password is URL-encoded
    assert conninfo.endswith("@localhost:5432/dalgo")
