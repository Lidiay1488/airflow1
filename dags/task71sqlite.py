import datetime
# модуль для работы с датами
import pendulum
import os
# модуль для выполнеения http запросов
import requests
from airflow.decorators import dag, task
# hooks - для взаимодействия с другими ресурсами
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

@dag(
    dag_id="process-employees1_lite",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
# dagrun_timeout=datetime.timedelta(minutes=60) - даг прерывается, если он не завершил исколнение через это время
def ProcessEmployees():
    create_employees_table = SqliteOperator(
        task_id="create_employees_table",
        sqlite_conn_id="sqligth_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );""",
    )
    create_employees_temp_table = SqliteOperator(
        task_id="create_employees_temp_table",
        sqlite_conn_id="sqligth_conn",
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );""",
    )
    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        # data_path = 'PATH_TO_FOLDER'
        data_path = './data/'
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
        response = requests.request("GET", url)
        with open(data_path + 'emp.csv', "w") as file:
            file.write(response.text)
        postgres_hook = SqliteHook(sqlite_conn_id="sqligth_conn")
        # postgres_hook - класс взаимодействия постргеса с airflow
        conn = postgres_hook.get_conn()
        # объект курсор необходим, чтоб далее выполнять sql-запросы
        cur = conn.cursor()
        with open(data_path + 'emp.csv', "r") as file:
            cur.copy_expert(
                "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()
        # commit отправляет изменения в бд
    @task
    def merge_data():
        query = """
            INSERT INTO employees
            SELECT *
            FROM (
            SELECT DISTINCT *
            FROM employees_temp
            )
            ON CONFLICT ("Serial Number") DO UPDATE
            SET "Serial Number" = excluded."Serial Number";
        """
        try:
            sqligth_hook = SqliteHook(sqlite_conn_id="sqligth_conn")
            conn = sqligth_hook.get_conn()
            cur = conn.cursor()
            cur.executescript(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1
    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()
dag = ProcessEmployees()

