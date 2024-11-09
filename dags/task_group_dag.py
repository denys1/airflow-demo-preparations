from datetime import datetime

from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


DAG_NAME = 'task_group'


def subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        schedule_interval="@daily",
    )

    for i in range(5):
        DummyOperator(
            task_id=f'{child_dag_name}-task-{i + 1}',
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id=DAG_NAME, default_args=args, start_date=days_ago(2), schedule_interval="@once", tags=['task_group', 'training']
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = DummyOperator(task_id="task_2")
        task_3 = DummyOperator(task_id="task_3")

    some_other_task = DummyOperator(
        task_id='some-other-task',
    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag(DAG_NAME, 'section-2', args),
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> section_1 >> some_other_task >> section_2 >> end
