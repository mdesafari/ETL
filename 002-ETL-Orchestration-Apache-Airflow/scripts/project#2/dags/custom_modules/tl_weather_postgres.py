import pandas as pd
import os
import errno
from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


def kelvin_to_degree_celsius(temp_in_kelvin):
    return temp_in_kelvin - 273.15


def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_degree_celsius(data["main"]["temp"])
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])

    transformed_data = {
        "city": city,
        "description": weather_description,
        "temperature": temp_celsius,
        "time": time_of_record 
    }
    
    return transformed_data

def insert_data_to_postgres_db(task_instance):
    transformed_data = task_instance.xcom_pull(task_ids='transform_weather_data')
    insert_sql = "INSERT INTO paris (city, description, temperature, time) VALUES (%s, %s, %s, %s)"
    pg_hook = PostgresHook(postgres_conn_id='postgres_id')
    pg_hook.run(
        insert_sql,
        parameters=(
            transformed_data['city'],
            transformed_data['description'],
            transformed_data['temperature'],
            transformed_data['time']
        )
    )
