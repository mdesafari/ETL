import pandas as pd
import os
import errno
from datetime import timedelta, datetime

def kelvin_to_degree_celsius(temp_in_kelvin):
    return temp_in_kelvin - 273.15

def transform_load_data(task_instance):
    """
    params:
        task_instance: represents the state of the previous task.
            It provides access to data of the previous task using XCom (Cross-communication).
    """
    # use xcom_pull to get data of the previous task whose id is 'extract_weather_data'
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    # extracts the city name from the data.
    city = data["name"]

    # extracts the weather description from the data
    weather_description = data["weather"][0]['description']

    # converts the temperature in Kelvin to degree Celsius
    temp_celsius = kelvin_to_degree_celsius(data["main"]["temp"])

    # converts the "feels like" temperature in Kelvin to degree Celsius
    feels_like_celsius = kelvin_to_degree_celsius(data["main"]["feels_like"])

    # converts the minimum temperature in Kelvin to degree Celsius
    min_temp_celsius = kelvin_to_degree_celsius(data["main"]["temp_min"])

    # converts the maximum temperature in Kelvin to degree Celsius
    max_temp_celsius = kelvin_to_degree_celsius(data["main"]["temp_max"])

    # extracts the atmospheric pressure from the data
    pressure = data["main"]["pressure"]

    # extracts the humidity from the data
    humidity = data["main"]["humidity"]

    # extracts the wind speed from the data
    wind_speed = data["wind"]["speed"]

    # calculates the timestamp of the weather record, sunrise time, and sunset time
    # I deliberately introduced an error in `time_of_record = datetime...`
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'], tz=datetime.timezone.utc)
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])

    # creates a dictionary transformed_data containing the transformed data
    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_celsius,
                        "Feels Like (F)": feels_like_celsius,
                        "Minimun Temp (F)":min_temp_celsius,
                        "Maximum Temp (F)": max_temp_celsius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    
    # creates a list transformed_data_list containing the transformed data dictionary
    transformed_data_list = [transformed_data]

    # converts the transformed data list into a pandas DataFrame.
    df_data = pd.DataFrame(transformed_data_list)

    # get the current date components
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day

    # save the DataFrame as a CSV file with a dynamically generated filename
    # to a local storage (in our airflow directory).
    # In /opt/airflow we will create a directrory named 'weather' and a subdirectory with the city's name
    data_dir = f'/opt/airflow/weather/{city}'

    # create the directory path if it does not exist
    try:
        os.makedirs(data_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    
    # saving path
    path_save = f'{data_dir}/weather_{year}_{month}_{day}.csv'

    # save the dataframe (in csv format)
    df_data.to_csv(path_save, index=False)