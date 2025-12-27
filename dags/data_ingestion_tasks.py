from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
import pandas as pd
import requests

# TO DO: Add data ingestion tasks
# 1. Create a separate task for retrieving data from .csv and .excel and creating pandas DF
# 2. Create a separate task to retrieve data from this API and creating pandas DF
# 3. Push each pandas DF to Xcom

default_args = {
    'owner': 'netever',
    'description': 'Data ingestion pipeline',
    'start_date': datetime(2024, 1, 1)
}
@dag(default_args=default_args, 
    dag_id='data_ingestion_pipeline',
    schedule_interval=None,  # Только ручной запуск
    catchup=False
)
def data_ingestion_pipeline():
    
    @task_group(group_id='Extract')
    def extract_group():
        
        @task
        def read_csv_task():
            df = pd.read_csv('/opt/airflow/dags/comments.csv', sep=';')
            return df.to_dict('records')

        @task
        def read_excel_task():
            df = pd.read_excel('/opt/airflow/dags/users.xlsx')
            return df.to_dict('records')

        @task
        def fetch_api_data():
            response = requests.get('https://jsonplaceholder.typicode.com/posts')
            df = pd.DataFrame(response.json())
            return df.to_dict('records')

        task_csv = read_csv_task()
        task_excel = read_excel_task()
        task_api = fetch_api_data()
        
        [task_csv, task_excel, task_api]  # Параллельное выполнение
        
        return [task_csv, task_excel, task_api]
    
    # Вызываем task group
    extract_tasks = extract_group()


data_ingestion_pipeline_dag = data_ingestion_pipeline()