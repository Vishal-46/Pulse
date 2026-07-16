from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# This function is the "work" our task performs.
def hello_function():
    print("Hello from the Airflow manager!")

# The DAG defines the schedule and the structure of our tasks.
with DAG(
    dag_id='test_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # This is a single task in our DAG.
    task1 = PythonOperator(
        task_id='hello_task',
        python_callable=hello_function
    )
