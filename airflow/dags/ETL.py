from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from darsie_scrape import scrape_result
from darsie_process import upload_data_temp
from darsie_load import load_data

default_args = {
    'owner':'Josias',
    'retries':'1',
    'retry_delay':timedelta(minutes=1)
    
}

def prueba():
    return 'todo bien'

with DAG(
    'ETL_test_2',
    default_args= default_args,
    description = 'extracting, transforming and loading data from darsie open API',
    start_date = datetime(2023,2,1),
    schedule_interval='@daily' 
) as dag:
    task1 = PythonOperator(
        task_id='extract',
        python_callable = prueba,
        dag=dag
    )
    # task2 = PythonOperator(
    #      task_id='process',
    #      python_callable = upload_data_temp,
    #      dag=dag
    #  )
    # task3 = PythonOperator(
    #      task_id = 'load',
    #      python_callable=load_data,
    #      dag=dag
    #  )
    
    task1 #>> task2 >> task3