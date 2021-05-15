from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from operators import DailyData, VacData


default_args = {
    'owner': 'ArnavGohil',
    'start_date': datetime.now(),
    'depends_on_past': False,   
    'email': ['arnav.gohil04@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'catchup':False
}

dag = DAG('Delhi_Covid_ETL',
          default_args=default_args,
          description='Near Real Time Data Processing',
          max_active_runs=1,
          schedule_interval='@once'
        )

file = "{}-centers.json".format(date.strftime("%d-%m-%Y"))
daily_data = dict()


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


get_data = PythonOperator(
	task_id='get_data', 
	dag=dag,
	python_callable = DailyData.main,
	op_kwargs={'data_dict': daily_data},
	)


put_data = DummyOperator(task_id='put_data',  dag=dag)


get_centers = PythonOperator(
	task_id='get_centers', 
	dag=dag,
	python_callable = DailyData.main,
	op_kwargs={'file_name': file},)


put_centers = DummyOperator(task_id='put_centers',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [get_data,get_centers]
get_data >> put_data >> end_operator
get_centers >> put_centers >> end_operator