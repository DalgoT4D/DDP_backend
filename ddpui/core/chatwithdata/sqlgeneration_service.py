import os
import tempfile
import json
from functools import lru_cache

from vanna.openai import OpenAI_Chat
from vanna.pgvector import PG_VectorStore

from ddpui.models.org import OrgWarehouse
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType
from ddpui.utils import secretsmanager


class DalgoVannaClient(PG_VectorStore, OpenAI_Chat):
    def __init__(self, openai_config={}):
        PG_VectorStore.__init__(
            self,
            config={
                "connection_string": "postgresql+psycopg://{username}:{password}@{server}:{port}/{database}".format(
                    **{
                        "username": os.environ["PGVECTOR_USER"],
                        "password": os.environ["PGVECTOR_PASSWORD"],
                        "server": os.environ["PGVECTOR_HOST"],
                        "port": os.environ["PGVECTOR_PORT"],
                        "database": os.environ["PGVECTOR_DB"],
                    }
                )
            },
        )
        OpenAI_Chat.__init__(
            self,
            config={
                "api_key": os.environ["OPENAI_API_KEY"],
                "model": "gpt-4o-mini",
                **openai_config,
            },
        )


class SqlGeneration:
    def __init__(self, warehouse: OrgWarehouse, config=None):
        self.vanna = DalgoVannaClient(
            openai_config={
                "initial_prompt": "Please qualify all table names with their schema names in the generated SQL"
            }
        )
        warehouse_creds = secretsmanager.retrieve_warehouse_credentials(warehouse)
        if warehouse.wtype == WarehouseType.POSTGRES:
            required_creds = {
                "host": warehouse_creds["host"],
                "port": warehouse_creds["port"],
                "dbname": warehouse_creds["database"],
                "user": warehouse_creds["username"],
                "password": warehouse_creds["password"],
            }

            # Extract remaining keys
            remaining_creds = {
                k: v
                for k, v in warehouse_creds.items()
                if k not in ["host", "port", "database", "username", "password"]
            }

            self.vanna.connect_to_postgres(**required_creds)
        elif warehouse.wtype == WarehouseType.BIGQUERY:
            cred_file_path = None
            with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as temp_file:
                json.dump(warehouse_creds, temp_file, indent=4)
                cred_file_path = temp_file.name

            self.vanna.connect_to_bigquery(
                project_id=warehouse_creds["project_id"],
                cred_file_path=cred_file_path,
            )
        else:
            raise ValueError("Invalid warehouse type")

    def generate_questions(self):
        return self.vanna.generate_questions()

    def generate_sql(self, question: str):
        return self.vanna.generate_sql(
            question=question,
        )

    def is_sql_valid(self, sql: str):
        return self.vanna.is_sql_valid(sql=sql)

    def run_sql(self, sql: str):
        return self.vanna.run_sql(sql=sql)

    def should_generate_chart(self, df):
        return self.vanna.should_generate_chart(df=df)

    def generate_plotly_code(self, question, sql, df):
        return self.vanna.generate_plotly_code(question=question, sql=sql, df=df)

    def generate_plot(self, code, df):
        return self.vanna.get_plotly_figure(plotly_code=code, df=df)

    def generate_followup(self, question, sql, df):
        return self.vanna.generate_followup_questions(question=question, sql=sql, df=df)

    def generate_summary(self, question, df):
        return self.vanna.generate_summary(question=question, df=df)

    def setup_training_plan_and_execute(self):
        df_information_schema = self.vanna.run_sql("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
        plan = self.vanna.get_training_plan_generic(df_information_schema)
        self.vanna.train(plan=plan)
        return True

    def remove_training_data(self):
        self.vanna.remove_training_data()
        return True
