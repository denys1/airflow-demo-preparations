import logging

from datetime import datetime
from uuid import uuid4

from airflow import DAG
from airflow.models import XCom, Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from custom_operators.postgre_sql_count_rows import PostgreSQLCountRows


logger = logging.getLogger()


def log(dag_id, database_name):
    logger.info(f'{dag_id} starts processing tables in database: {database_name}')

def check_table_exist(database_name, table_name):
    hook = PostgresHook()
    results = hook.get_records(sql=f"""SELECT * FROM information_schema.tables 
                                       WHERE table_schema = '{database_name}'
                                           AND table_name = '{table_name}'; """)
    if results:
        return 'dummy_task'
    logger.info(f'Table "{table_name}" was not found in "{database_name}" schema.')
    return 'create_table'


def xcom_push_execution_data(**context):
    xcom_key = f"{context['dag'].dag_id}_last_execution_date"
    payload = str(context['execution_date'])
    context['ti'].xcom_push(key=xcom_key, value=payload)
    logger.info(f'### Pushed date: {payload}')

    xcom_key = f"{context['dag'].dag_id}_last_run_id"
    payload = context['run_id']
    context['ti'].xcom_push(key=xcom_key, value=payload)
    logger.info(f'### Pushed run_id: {payload}')


configs = {
   'dag_id_1': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), "database_name": "public", "table_name": "custom_table", 'tags': ['first_dag', 'training']},
   'dag_id_2': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), "database_name": "public", "table_name": "custom_table", 'tags': ['second_dag', 'training']},
   'dag_id_3': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), "database_name": "public", "table_name": "custom_table", 'tags': ['third_dag', 'training']}
}


for dag_id, config in configs.items():
    with DAG(
        dag_id,
        schedule_interval=config['schedule_interval'],
        start_date=config['start_date'],
        tags=config['tags']
    ) as dag:

        print_process_start = PythonOperator(
            task_id='print_process_start',
            python_callable=log,
            op_args=[dag_id, config['database_name']]
        )

        get_current_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami',
            do_xcom_push=True
        )

        check_table_exists = BranchPythonOperator(
            task_id='check_table_exists',
            python_callable=check_table_exist,
            op_args=[ config['database_name'], config['table_name'] ]
        )

        create_table = PostgresOperator(
            task_id='create_table',
            sql=f"""CREATE TABLE {config['database_name']}.{config['table_name']}
                    (custom_id integer NOT NULL,
                     user_name VARCHAR (50) NOT NULL, 
                     timestamp TIMESTAMP NOT NULL);"""
        )

        dummy_task = DummyOperator(
            task_id='dummy_task'
        )

        insert_row = PostgresOperator(
            task_id='insert_row',
            trigger_rule=TriggerRule.NONE_FAILED,
            sql=f'''INSERT INTO {config['database_name']}.{config['table_name']}
                    VALUES (%s, '{{{{ ti.xcom_pull(task_ids='get_current_user', key='return_value', dag_id='{dag_id}') }}}}', %s); ''',
            parameters=(uuid4().int % 123456789, datetime.now())
        )

        get_row_count = PostgreSQLCountRows(
            task_id='get_row_count',
            database_name=config['database_name'],
            table_name=config['table_name']
        )

        store_execution_data = PythonOperator(
            task_id='store_execution_data',
            python_callable=xcom_push_execution_data
        )


        print_process_start >> get_current_user >> check_table_exists
        check_table_exists >> create_table >> insert_row >> get_row_count >> store_execution_data
        check_table_exists >> dummy_task >> insert_row >> get_row_count >> store_execution_data

        globals()[dag_id] = dag
