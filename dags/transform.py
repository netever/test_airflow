from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
import pandas as pd
import requests

# TO DO: Add union data tasks
# 1. task should pull data from XCom from each task group which you create 
# 2. inside task you should join all 3 DataFrames (analyze how you can join them) and get final DataFrame which will have users name, surname, email, company name, website, phone, company address, content of comment(body)
# (be careful some DF’s have json data inside columns and don’t forget to deduplicate result DF)
# 3. push result DF to XCom

default_args = {
    'owner': 'netever',
    'description': 'Union data pipeline',
    'start_date': datetime(2024, 1, 1)
}
@dag(default_args=default_args, 
    dag_id='union_data_pipeline',
    schedule_interval=None,  # Только ручной запуск
    catchup=False
)
def union_data_pipeline():
    
    @task_group(group_id='Transform')
    def transform_group():
        
        @task
        def union_data_task(ti):
            # 1. Получаем данные из XCom
            print("=" * 50)
            print("Начинаем получать данные из XCom...")
            
            # ВАЖНО: Указываем dag_id для cross-DAG communication
            xcom_csv = ti.xcom_pull(
                task_ids='Extract.read_csv_task',
                dag_id='data_ingestion_pipeline',
                include_prior_dates=True
            )
            xcom_excel = ti.xcom_pull(
                task_ids='Extract.read_excel_task',
                dag_id='data_ingestion_pipeline',
                include_prior_dates=True
            )
            xcom_api = ti.xcom_pull(
                task_ids='Extract.fetch_api_data',
                dag_id='data_ingestion_pipeline',
                include_prior_dates=True
            )
            
            print(f"XCom CSV type: {type(xcom_csv)}, length: {len(xcom_csv) if xcom_csv else 0}")
            print(f"XCom Excel type: {type(xcom_excel)}, length: {len(xcom_excel) if xcom_excel else 0}")
            print(f"XCom API type: {type(xcom_api)}, length: {len(xcom_api) if xcom_api else 0}")
            
            if not xcom_csv:
                raise ValueError("XCom CSV пустой! Убедитесь, что DAG 'data_ingestion_pipeline' был запущен и успешно выполнен.")
            if not xcom_excel:
                raise ValueError("XCom Excel пустой! Убедитесь, что DAG 'data_ingestion_pipeline' был запущен и успешно выполнен.")
            if not xcom_api:
                raise ValueError("XCom API пустой! Убедитесь, что DAG 'data_ingestion_pipeline' был запущен и успешно выполнен.")
            
            df_csv = pd.DataFrame(xcom_csv)
            df_excel = pd.DataFrame(xcom_excel)
            df_api = pd.DataFrame(xcom_api)
            
            print(f"\nCSV DataFrame: {df_csv.shape}")
            print(f"CSV columns: {df_csv.columns.tolist()}")
            
            print(f"\nExcel DataFrame: {df_excel.shape}")
            print(f"Excel columns: {df_excel.columns.tolist()}")
            
            print(f"\nAPI DataFrame: {df_api.shape}")
            print(f"API columns: {df_api.columns.tolist()}")
            print("=" * 50)

            # 2. Распаковываем вложенные JSON объекты из Excel (users.xlsx содержит JSON)
            # После XCom вложенные словари могут стать строками, используем eval для преобразования
            import ast
            
            # Извлекаем данные из company
            df_excel['company_name'] = df_excel['company'].apply(
                lambda x: x['name'] if isinstance(x, dict) else ast.literal_eval(x)['name'] if isinstance(x, str) else None
            )
            
            # Извлекаем данные из address
            df_excel['company_address'] = df_excel['address'].apply(
                lambda x: f"{x['street']}, {x['suite']}, {x['city']}, {x['zipcode']}" 
                if isinstance(x, dict) 
                else f"{ast.literal_eval(x)['street']}, {ast.literal_eval(x)['suite']}, {ast.literal_eval(x)['city']}, {ast.literal_eval(x)['zipcode']}"
                if isinstance(x, str)
                else None
            )
            
            # Извлекаем имя и фамилию из поля name (если есть пробел)
            df_excel['surname'] = df_excel['name'].apply(lambda x: x.split()[-1] if ' ' in str(x) else '')
            df_excel['name'] = df_excel['name'].apply(lambda x: x.split()[0] if ' ' in str(x) else x)
            
            # 3. Объединяем DataFrame'ы через JOIN
            # Шаг 1: Объединяем Excel (users) с API (posts) по id (Excel) = userId (API)
            df_step1 = pd.merge(
                df_excel, 
                df_api[['userId', 'id', 'title', 'body']],
                left_on='id',
                right_on='userId',
                how='left',
                suffixes=('_user', '_post')
            )
            
            # Шаг 2: Объединяем результат с CSV (comments) по id_post = id (CSV)
            # Сначала переименовываем колонки для ясности
            df_csv_renamed = df_csv.rename(columns={'id': 'comment_id', 'title': 'comment_title'})
            
            df_merged = pd.merge(
                df_step1,
                df_csv_renamed[['userId', 'comment_title']],
                left_on='userId',
                right_on='userId',
                how='left'
            )
            
            # Объединяем body из API и comment из CSV в одну колонку
            # Предпочитаем body из API, если нет - берем comment из CSV
            df_merged['content_of_comment'] = df_merged['body'].fillna(df_merged['comment_title'])
            
            # 4. Выбираем только нужные колонки согласно заданию
            df_final = df_merged[[
                'name',                # имя пользователя
                'surname',             # фамилия пользователя
                'email',               # email
                'company_name',        # название компании
                'website',             # сайт
                'phone',               # телефон
                'company_address',     # адрес компании
                'content_of_comment'   # содержимое комментария (body)
            ]].copy()
            
            # 5. ДЕДУПЛИКАЦИЯ - удаляем дубликаты по определенным колонкам
            print(f"Записей до дедупликации: {len(df_final)}")
            df_final = df_final.drop_duplicates(subset=['email', 'content_of_comment'], keep='first')
            print(f"Записей после дедупликации: {len(df_final)}")
            
            # Удаляем строки где нет комментариев/постов
            df_final = df_final.dropna(subset=['content_of_comment'])
            print(f"Записей после удаления пустых комментариев: {len(df_final)}")
            
            return df_final.to_dict('records')
        
        union_data_task = union_data_task()
        
        return union_data_task
    
    # Вызываем task group
    transform_tasks = transform_group()

# Создаем экземпляр DAG
union_data_pipeline_dag = union_data_pipeline()
