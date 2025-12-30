from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import ast

# Глобальная переменная для отладки
DEBUG = True

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
    
    @task_group(group_id='union_data')
    def union_data_group():
        
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

        @task
        def union_data_task(ti):
            # Получаем данные из XCom (внутри task_group можно указывать просто имя task)
            df_csv = pd.DataFrame(ti.xcom_pull(task_ids='union_data.read_csv_task'))
            df_excel = pd.DataFrame(ti.xcom_pull(task_ids='union_data.read_excel_task'))
            df_api = pd.DataFrame(ti.xcom_pull(task_ids='union_data.fetch_api_data'))
            
            if DEBUG:
                print(f"CSV columns: {df_csv.columns.tolist()}")
                print(f"Excel columns: {df_excel.columns.tolist()}")
                print(f"API columns: {df_api.columns.tolist()}")
            
            # Распаковываем вложенные JSON объекты из Excel
            df_excel['company_name'] = df_excel['company'].apply(
                lambda x: x['name'] if isinstance(x, dict) else ast.literal_eval(x)['name'] if isinstance(x, str) else None
            )
            df_excel['company_address'] = df_excel['address'].apply(
                lambda x: f"{x['street']}, {x['suite']}, {x['city']}, {x['zipcode']}" 
                if isinstance(x, dict) 
                else f"{ast.literal_eval(x)['street']}, {ast.literal_eval(x)['suite']}, {ast.literal_eval(x)['city']}, {ast.literal_eval(x)['zipcode']}"
                if isinstance(x, str)
                else None
            )
            
            # Извлекаем имя и фамилию
            df_excel['surname'] = df_excel['name'].apply(lambda x: x.split()[-1] if ' ' in str(x) else '')
            df_excel['name'] = df_excel['name'].apply(lambda x: x.split()[0] if ' ' in str(x) else x)
            
            # Объединяем DataFrame'ы через JOIN
            # Шаг 1: Excel (users) + API (posts) по id (Excel) = userId (API)
            df_step1 = pd.merge(
                df_excel, 
                df_api[['userId', 'id', 'title', 'body']],
                left_on='id',
                right_on='userId',
                how='left',
                suffixes=('_user', '_post')
            )
            
            # Шаг 2: Результат + CSV (comments) по userId
            df_csv_renamed = df_csv.rename(columns={'title': 'comment_title'})
            df_merged = pd.merge(
                df_step1,
                df_csv_renamed[['userId', 'comment_title']],
                left_on='userId',
                right_on='userId',
                how='left'
            )
            
            # Объединяем body из API и comment из CSV
            df_merged['content_of_comment'] = df_merged['body'].fillna(df_merged['comment_title'])
            
            # Выбираем нужные колонки
            df_final = df_merged[[
                'name', 'surname', 'email', 'company_name', 
                'website', 'phone', 'company_address', 'content_of_comment'
            ]].copy()
            
            # Дедупликация
            if DEBUG:
                print(f"Записей до дедупликации: {len(df_final)}")
            df_final = df_final.drop_duplicates(subset=['email', 'content_of_comment'], keep='first')
            if DEBUG:
                print(f"Записей после дедупликации: {len(df_final)}")
            
            # Удаляем пустые
            df_final = df_final.dropna(subset=['content_of_comment'])
            if DEBUG:
                print(f"Записей после удаления пустых: {len(df_final)}")
            
            return df_final.to_dict('records')

        
        task_csv = read_csv_task()
        task_excel = read_excel_task()
        task_api = fetch_api_data()
        task_union = union_data_task()
        
        [task_csv, task_excel, task_api] >> task_union
        
        return task_union
    
    # Вызываем task group
    union_data_tasks = union_data_group()


data_ingestion_pipeline_dag = data_ingestion_pipeline()