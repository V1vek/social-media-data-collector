from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime
from datetime import timedelta

import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from lib import google_sheet, facebook


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'facebook_history',
    default_args=default_args,
    description='Collect and store twitter search data',
    schedule_interval=None
)

get_config = PythonOperator(
    task_id='get_config',
    provide_context=True,
    python_callable=google_sheet.get_config,
    op_kwargs={'worksheet': 'config', 'platform': 'Facebook', 'type': 'history'},
    dag=dag,
)

collect_history_data = PythonOperator(
    task_id='collect_history_data',
    provide_context=True,
    python_callable=facebook.get_posts_data,
    dag=dag,
)

get_config >> collect_history_data
