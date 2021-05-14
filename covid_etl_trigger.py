from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import stage_redshift


default_args = {
    'owner': 'ArnavGohil',
    'start_date': datetime.now(),
    'depends_on_past': False,   
    'email': ['arnav.gohil04@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False
}

dag = DAG('Delhi_Covid_ETL',
          default_args=default_args,
          description='Final Project',
          max_active_runs=1,
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

get_tweet = DummyOperator(task_id='get_tweet',  dag=dag)
get_tweet_text = DummyOperator(task_id='get_tweet_text',  dag=dag)
parse_tweet = DummyOperator(task_id='parse_tweet',  dag=dag)
put_data = DummyOperator(task_id='put_data',  dag=dag)
get_centers = DummyOperator(task_id='get_centers',  dag=dag)
put_centers = DummyOperator(task_id='put_centers',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [get_tweet,get_centers]
get_tweet >> get_tweet_text >> parse_tweet >> put_data >> end_operator
get_centers >> put_centers >> end_operator