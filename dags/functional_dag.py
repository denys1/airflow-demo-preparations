import logging
import pandas as pd
import urllib.request
import os

from datetime import datetime

from airflow import DAG
from airflow.operators.python import task


FILE_DOWNLOAD_URL = os.environ.get('FILE_DOWNLOAD_URL')
if not FILE_DOWNLOAD_URL:
    raise ValueError("FILE_DOWNLOAD_URL environment variable is not set")

FILE_LOCAL_PATH = os.environ.get('FILE_LOCAL_PATH')
if not FILE_LOCAL_PATH:
    raise ValueError("FILE_LOCAL_PATH environment variable is not set")
logger = logging.getLogger()


dag_config = {
    'dag_id': 'functional_dag',
    'schedule_interval': None,
    'start_date': datetime(2021, 9, 11),
    'tags': ['etl', 'training']
}



def _get_local_path_with_ts(path, ts):
    path = path.split('.')
    path.insert(-1, f'_{ts}.')
    return ''.join(path)

@task(queue='fs_queue', multiple_outputs=True)
def download_file(download_url, file_local_path, ts_nodash):
    file_local_path_with_ts = _get_local_path_with_ts(file_local_path, ts_nodash)
    response = urllib.request.urlretrieve(download_url, file_local_path_with_ts)
    return { 
        'file_path': response[0],
        'metadata': str(response[1])
    }

@task(queue='fs_queue')
def count_accidents(load_info):
    df = pd.read_csv(load_info['file_path'], encoding='windows-1252')
    df = df['Year'].groupby(df.Year).count().reset_index(name='Count')
    return str(df)

@task
def print_results(counts_data):
    logger.info('Incidents count per year: ')
    logger.info(f'\n{counts_data}')


with DAG(
    dag_id=dag_config['dag_id'],
    schedule_interval=dag_config['schedule_interval'],
    start_date=dag_config['start_date'],
    tags=dag_config['tags'],
    max_active_runs=1
) as dag:

    load = download_file(
        download_url=FILE_DOWNLOAD_URL,
        file_local_path=FILE_LOCAL_PATH,
        ts_nodash="{{ ts_nodash }}"
    )

    count = count_accidents(
        load_info=load
    )

    print = print_results(
        counts_data=count
    )

    load >> count >> print
