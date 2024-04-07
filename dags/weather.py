from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

sem8dag = DAG( 'sem8dag', description= 'sem8dag',
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2021,4,1),
catchup= False )

def get_weather_data(ti = None):
    # open_weather_url = "https://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=cde25583263075ff71ffa3961bd239d9"
    # response = requests.get(open_weather_url)
    # temperature_openweather = response.json()['main']['temp'] - 273
    # ti.xc
    
    yandex_url = 'https://api.weather.yandex.ru/v2/forecast?lat=51.5085&lon=0.12574'
    headers = {
    'X-Yandex-Weather-Key': "30931d55-9e3a-4995-8e24-3ace0bbee393"
    }
    response = requests.get(url=yandex_url, headers=headers)
    temperature_yandex = response.json()['fact']['temp']
    ti.xcom_push(key="temperature_yandex", value=temperature_yandex)


get_weather = PythonOperator(task_id = "get_weather_data",python_callable=get_weather_data,dag = sem8dag)


send_tg_message = TelegramOperator(
    task_id='send_telega', 
    token="6386041499:AAHdtdhAMXq9TZmNqQ4_POeC-XX5veHDmsI", 
    chat_id=533785491, 
    text= 'Температура в Лондоне по версии Yandex: \
        {{ ti.xcom_pull(key="temperature_yandex", task_ids="get_weather_data") }}',
    dag=sem8dag  
)

get_weather >> send_tg_message
