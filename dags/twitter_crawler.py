from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime
from datetime import timedelta

import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from lib import google_sheet, twitter


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
    'twitter_crawler',
    default_args=default_args,
    description='Collect and store twitter search data',
    schedule_interval='0 0 * * *'
)

get_config = PythonOperator(
    task_id='get_config',
    provide_context=True,
    python_callable=google_sheet.get_config,
    op_kwargs={'worksheet': 'config', 'platform': 'Twitter', 'type': 'daily'},
    dag=dag,
)

collect_data = PythonOperator(
    task_id='collect_data',
    provide_context=True,
    python_callable=twitter.collect_data,
    dag=dag,
)

# collect_replies = PythonOperator(
#     task_id='collect_replies',
#     provide_context=True,
#     python_callable=twitter.collect_replies,
#     dag=dag,
# )

get_config >> collect_data
# collect_data >> collect_replies
