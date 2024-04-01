from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from custom_modules.transform_load_weather_funcs import transform_load_data


default_args = {
    'owner': 'mdesafari',  #  owner of the DAG
    'depends_on_past': False, # whether a task depends on the success of its previous runs (True) or not (False)
    'start_date': datetime(2024, 4, 1),  # start date of the DAG. Tasks will only be scheduled after this date.
    'end_date': datetime(2024, 4, 7),  # end date for the DAG
    'email': ['your_email@gmail.com'],  # (optional) list of email addresses to receive notifications
    'email_on_failure': False, # whether email notifications should be sent on task failure (True) or not (False).
    'email_on_retry': False,  # whether email notifications should be sent on task retry after failure (True) or not (False).
    'retries': 2,  # max number of retry attempts in case of task failure
    'retry_delay': timedelta(minutes=2),  # time interval between retry attempts for failed tasks
    'schedule_interval': '0 22 * * *',  # DAG will run at 22:00 (10 PM) UTC every day. (more details below)
    'catchup': False  # wether to execute missed previous tasks during the initial scheduling (or after a change in default_args) of the DAG or not.
}


# 1. CREATE THE DAG DEFINITION
dag = DAG(
    dag_id='weather_dag',
    default_args=default_args
)

# 2. CREATE TASKS
# 2.1. Task 1: Check if the weather API is ready
api_key = ''
city_name = 'Paris'
endpoint = f'/data/2.5/weather?q={city_name}&appid={api_key}'

is_weather_api_ready = HttpSensor(
    task_id ='is_weather_api_ready',  # unique identifier for the task
    http_conn_id='weathermap_api',  # Airflow connection ID, which contains the configuration details for the HTTP connection to the weather API.
    endpoint=endpoint,  # API endpoint path to query weather data for Portland, along with the API key (APPID) needed for authentication.
    dag=dag
)

# 2.2. Task 2: Extract weather data from the API
extract_weather_data = SimpleHttpOperator(  # operator used to make an HTTP request to extract weather data
    task_id = 'extract_weather_data',  # unique identifier for this task in the DAG
    http_conn_id = 'weathermap_api',  # id of the HTTP connection to be used for the request (we set it in airflow)
    endpoint=endpoint,  # URL endpoint to which the HTTP request will be made
    method = 'GET',  # The operator will perform a GET request to retrieve weather data
    response_filter= lambda r: json.loads(r.text),  # a lambda function that converts the response text into a JSON object
    log_response=True,  # nables logging of the response received. Response details will be logged in the Airflow task logs
    dag=dag
)

# 2.3. Task 3: transform and load weather data
transform_load_weather_data = PythonOperator(
    task_id= 'transform_load_weather_data',
    python_callable=transform_load_data,
    dag=dag
)

# 3. TASKS DEPENDENCIES
is_weather_api_ready >> extract_weather_data >> transform_load_weather_data


