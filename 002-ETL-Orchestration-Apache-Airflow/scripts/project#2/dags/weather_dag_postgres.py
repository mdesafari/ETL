from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from custom_modules.tl_weather_postgres import transform_data, insert_data_to_postgres_db


default_args = {
    'owner': 'mdesafari',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'end_date': datetime(2024, 4, 7),
    'email': ['your_email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'schedule_interval': '0 22 * * *',
    'catchup': False
}

# 1. CREATE THE DAG DEFINITION
dag = DAG(
    dag_id='weather_dag_postgres',
    default_args=default_args
)

# 2. CREATE TASKS
# 2.1. Task 1: Check if the weather API is ready
api_key = ''
city_name = 'Paris'
endpoint = f'/data/2.5/weather?q={city_name}&appid={api_key}'

is_weather_api_ready = HttpSensor(
    task_id ='is_weather_api_ready',
    http_conn_id='weathermap_api',
    endpoint=endpoint,
    dag=dag
)

# 2.2. Task 2: Extract weather data from the API
extract_weather_data = SimpleHttpOperator(
    task_id = 'extract_weather_data',
    http_conn_id = 'weathermap_api',
    endpoint=endpoint,
    method = 'GET',
    response_filter= lambda r: json.loads(r.text),
    log_response=True,
    dag=dag
)

# 2.3. Task 3: transform and load weather data
transform_weather_data = PythonOperator(
    task_id= 'transform_weather_data',
    python_callable=transform_data,
    dag=dag
)


# 3. creata postgres tasks
# 3.1. create table
create_weather_table = PostgresOperator(
    task_id='create_weather_table',
    postgres_conn_id='postgres_id',
    sql='''CREATE TABLE IF NOT EXISTS paris (
        city VARCHAR(255),
        description VARCHAR(255),
        temperature FLOAT,
        time TIMESTAMP
    );''',
    dag=dag
)


# 3.2. insert data
insert_data_to_postgres = PythonOperator(
    task_id='insert_data_to_postgres',
    python_callable=insert_data_to_postgres_db,
    provide_context=True,
    dag=dag
)


# 4. TASKS DEPENDENCIES
is_weather_api_ready >> extract_weather_data >> transform_weather_data
create_weather_table << is_weather_api_ready
insert_data_to_postgres << transform_weather_data
insert_data_to_postgres << create_weather_table
