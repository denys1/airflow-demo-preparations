import logging

from datetime import datetime, timedelta

from airflow import DAG
from custom_operators.smart_file_sensor import SmartFileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# from airflow.hooks.base import BaseHook
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


logger = logging.getLogger()
RUN_FILE_PATH = Variable.get('run_file_path_variable', default_var='run')
FS_CONNECTION_ID = 'custom_fs_connection'
SLACK_CHANNEL = 'ch1'
DAG_TO_TRIGGER = 'dag_id_1'


dag_config = {
    'dag_id': 'sensor_dag',
    'schedule_interval': None,
    'start_date': datetime(2021, 9, 11),
    'tags': ['sensor_dag', 'training']
}


def send_slack_message(dag_id, execution_timestamp):
    slack_token = Variable.get('slack_token')
    client = WebClient(token=slack_token)

    try:
        response = client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"Dag: '{dag_id}' has finished at {execution_timestamp}. :tada:"
        )
        logger.info(f'Response: {response}')
    except SlackApiError as e:
        assert e.response["error"]

def print_execution_result(**context):
    result = context['ti'].xcom_pull(
        key='dag_id_1_last_run_id',
        task_ids='store_execution_data',
        dag_id='dag_id_1',
        include_prior_dates=True
    )
    logger.info(f'{result} ended')
    logger.info(f'Whole context: {context}')


def subdag_process_results(external_dag_id, sub_dag_id, start_date, dag_to_trigger, external_task_id=None, schedule_interval=None):
    sub_dag = DAG(
        dag_id=f'{external_dag_id}.{sub_dag_id}',
        start_date=start_date,
        schedule_interval=schedule_interval
    )

    with sub_dag:
        sensor_triggered_dag = ExternalTaskSensor(
            task_id='sensor_triggered_dag',
            external_dag_id=dag_to_trigger,
            external_task_id=external_task_id,
            execution_delta=timedelta(seconds=0)
        )

        print_result = PythonOperator(
            task_id='print_result',
            python_callable=print_execution_result,
            provide_context=True
        )

        remove_run_file = BashOperator(
            task_id='remove_run_file',
            bash_command=f'rm -f /opt/airflow/run_file_folder/{RUN_FILE_PATH}',
            queue='default'
        )

        create_timestamp = BashOperator(
            task_id='create_timestamp',
            bash_command='touch /opt/airflow/run_file_folder/finished_{{ ts_nodash }}',
            queue='default'
        )

        sensor_triggered_dag >> print_result >> remove_run_file >> create_timestamp

    return sub_dag




with DAG(
    dag_id=dag_config['dag_id'],
    schedule_interval=dag_config['schedule_interval'],
    start_date=dag_config['start_date'],
    tags=dag_config['tags'],
    max_active_runs=1
) as dag:

    wait_file = SmartFileSensor(
        task_id='wait_file',
        filepath=RUN_FILE_PATH,
        fs_conn_id=FS_CONNECTION_ID
    )

    # TODO: Replace const variable to have possibility to call different dags
    # exmaple: trigger_dag_id='{{ dag_run.conf["dag_id"] }}'
    # we can pass this macros since 'execution_date' is templated in TriggerDagRunOperator
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_dag', 
        trigger_dag_id=DAG_TO_TRIGGER,
        wait_for_completion=True,
        poke_interval=5,
        reset_dag_run=True,
        execution_date='{{ execution_date }}'
    )

    process_results = SubDagOperator(
        task_id='process_results',
        subdag=subdag_process_results(
            external_dag_id=dag.dag_id,
            sub_dag_id='process_results',
            dag_to_trigger=DAG_TO_TRIGGER,
            schedule_interval=dag.schedule_interval,
            start_date=dag_config['start_date']
        )
    )

    alert_to_slack = PythonOperator(
        task_id='alert_to_slack',
        python_callable=send_slack_message,
        op_args=[ DAG_TO_TRIGGER, '{{ execution_date }}' ]
    )

    wait_file >> trigger_dag >> process_results >> alert_to_slack
