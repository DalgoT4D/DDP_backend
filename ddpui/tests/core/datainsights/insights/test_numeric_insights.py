import os
import django
from django.core.management import call_command
from django.apps import apps

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


import pytest
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    Float,
)
from sqlalchemy import inspect
from sqlalchemy.types import NullType

from ddpui.datainsights.insights.numeric_type.queries import DataStats
from ddpui.datainsights.insights.insight_interface import MAP_TRANSLATE_TYPES
from ddpui.datainsights.insights.numeric_type.numeric_insight import NumericColInsights


@pytest.fixture
def engine():
    my_engine = create_engine(
        "sqlite:///file:test_db?mode=memory&cache=shared&uri=true", echo=True
    )
    return my_engine


@pytest.fixture
def target_table(engine):
    meta = MetaData()
    table_name = "dummy_data"
    dummy_table = Table(
        table_name,
        meta,
        Column("id", Integer, primary_key=True),
        Column("c1", Float),
        Column("c2", String),
        Column("c3", DateTime),
    )
    meta.create_all(engine)

    records = [
        {
            "c1": 1,
            "c2": "somestring",
            "c3": "2024-04-26 06:20:29+00",
        },
        {
            "c1": 2,
            "c2": "somestring1",
            "c3": "2023-04-26 06:20:29+00",
        },
        {
            "c1": 3,
            "c2": "somestring12",
            "c3": "2022-04-26 06:20:29+00",
        },
    ]

    df = pd.DataFrame(records)
    df.to_sql(table_name, engine, if_exists="append", index=False)
    return dummy_table


def test_numeric_data_stats(engine, target_table):
    """Tests mean, media and mode values"""
    with Session(engine) as session:
        cols = []
        for column in inspect(engine).get_columns(
            table_name=target_table, schema="test_db"
        ):
            cols.append(
                {
                    "name": column["name"],
                    "data_type": str(column["type"]),
                    "translated_type": (
                        None
                        if isinstance(column["type"], NullType)
                        else MAP_TRANSLATE_TYPES[column["type"].python_type]
                    ),
                    "nullable": column["nullable"],
                }
            )
        insight = NumericColInsights(cols, target_table, "test_db")

        df = pd.read_sql_query(insight.insights[0].generate_sql(), session.bind)
        print(df)
