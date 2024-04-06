from datetime import datetime
from random import randint
from json import loads
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

def sq_rand_fig():
    num = randint(1, 100)
    return num**2

dag = DAG('sem6_hw', description='seminar6 homework',
          schedule='@daily',
          start_date=datetime(2024, 1, 1), catchup=False)


# 1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать 
# рандомное число и печатать его в консоль.
gen_rand_figure_operator = BashOperator(
    task_id='rand_fig_task', 
    bash_command='echo $((RANDOM % 100))', 
    dag=dag
    )


# 2. Создайте PythonOperator, который генерирует рандомное число, возводит его в квадрат 
# и выводит в консоль исходное число и результат.
sq_rand_figure_operator = PythonOperator(task_id='sq_rand_fig_task', python_callable=sq_rand_fig, dag=dag)


# 3. Сделайте оператор, который отправляет запрос 
# к https://goweather.herokuapp.com/weather/"location" (вместо location используйте ваше местоположение).
wether_operator = SimpleHttpOperator(
    task_id='wether_task',
    http_conn_id='wether_connect',
    endpoint='Paris',
    method='GET',
    response_filter=lambda response: loads(response.text),
    log_response=True,
    dag=dag
)

# 4. Задайте последовательный порядок выполнения операторов.
gen_rand_figure_operator >> sq_rand_figure_operator >> wether_operator
