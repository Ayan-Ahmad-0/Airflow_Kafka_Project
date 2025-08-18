from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='Taxi_data_pipeline',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  
    catchup=False,
    description='Produce and consume NYC taxi data using Kafka and store in PostgreSQL',
) as dag:

    produce_data = BashOperator(
    task_id='run_kafka_producer',
    bash_command='python /opt/airflow/scripts/kproducer.py || exit 1',
    do_xcom_push=True,
)

    consume_data = BashOperator(
        task_id='run_kafka_consumer',
        bash_command='python /opt/airflow/scripts/consumer_to_postgres.py',
        execution_timeout=timedelta(minutes=5),
    )

    produce_data >> consume_data
