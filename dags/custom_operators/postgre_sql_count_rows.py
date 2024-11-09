from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRows(BaseOperator):
    def __init__(self, database_name: str, table_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.database_name = database_name
        self.table_name = table_name

    def execute(self, context):
        print('### Started PostgreSQLCountRows')
        hook = PostgresHook()
        results = hook.get_first(sql=f"""SELECT COUNT(1) FROM {self.database_name}.{self.table_name}; """)
        context['ti'].xcom_push(key=f"{context['dag'].dag_id}_rows_number", value=results[0])
        print('### result: ', results[0])
        print('### Finished PostgreSQLCountRows')
