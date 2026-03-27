"""Official LangGraph Postgres checkpoint wiring for dashboard chat."""

from dataclasses import dataclass
from functools import lru_cache
import os

from django.conf import settings
from psycopg import Connection
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from langgraph.checkpoint.postgres import PostgresSaver


@dataclass(frozen=True)
class DashboardChatCheckpointConfig:
    """Configuration for dashboard chat LangGraph checkpoint persistence."""

    conninfo: str

    @classmethod
    def from_env(cls) -> "DashboardChatCheckpointConfig":
        """Build checkpoint configuration from env or Django DB settings."""
        conninfo = os.getenv("AI_DASHBOARD_CHAT_LANGGRAPH_CHECKPOINT_CONNINFO")
        if conninfo:
            return cls(conninfo=conninfo)

        default_db = settings.DATABASES["default"]
        return cls(
            conninfo=make_conninfo(
                dbname=default_db.get("NAME") or "",
                user=default_db.get("USER") or "",
                password=default_db.get("PASSWORD") or "",
                host=default_db.get("HOST") or "",
                port=str(default_db.get("PORT") or ""),
            )
        )


class DashboardChatCheckpointer:
    """Long-lived Postgres saver wrapper used by the shared dashboard chat runtime."""

    def __init__(self, config: DashboardChatCheckpointConfig):
        self.config = config
        self.connection = Connection.connect(
            config.conninfo,
            autocommit=True,
            prepare_threshold=0,
            row_factory=dict_row,
        )
        self.saver = PostgresSaver(self.connection)
        # LangGraph owns its checkpoint schema on this path.
        self.saver.setup()

    def close(self) -> None:
        """Close the underlying Postgres connection when tests/process shutdown need it."""
        if not self.connection.closed:
            self.connection.close()


@lru_cache(maxsize=1)
def get_dashboard_chat_checkpointer() -> DashboardChatCheckpointer:
    """Return the shared checkpoint wrapper for dashboard chat runtime persistence."""
    return DashboardChatCheckpointer(DashboardChatCheckpointConfig.from_env())


def reset_dashboard_chat_checkpointer() -> None:
    """Tear down the shared checkpointer so tests do not leak DB sessions."""
    if get_dashboard_chat_checkpointer.cache_info().currsize:
        get_dashboard_chat_checkpointer().close()
    get_dashboard_chat_checkpointer.cache_clear()
