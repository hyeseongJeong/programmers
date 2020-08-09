
from airflow import DAG
from airflow import PythonOperator
from datetime import datetime


default_dag = DAG(
    dag_id='my_first_dag',
    start_date=datetime(2020,8,7),
    schedule_interval='0 2 * * *')


def print_hello():
    return "hello!"


def print_goodbye():
    return "goodbye!"


print_hello_op = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=default_dag)


print_goodbye_op = PythonOperator(
    task_id='print_goodbye',
    python_callable=print_goodbye,
    dag=default_dag)


#Assign the order of the tasks in our DAG
print_hello_op >> print_goodbye_op