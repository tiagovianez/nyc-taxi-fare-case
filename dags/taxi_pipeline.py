from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime


kafka_conn = BaseHook.get_connection("kafka_default")
bootstrap_server = kafka_conn.extra_dejson.get("bootstrap.servers")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'taxi_stream_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

check_kafka = BashOperator(
    task_id='check_kafka',
    bash_command="until nc -zv localhost 9092; do echo 'Waiting for Kafka...'; sleep 5; done",
    dag=dag
)

spark_job = SparkSubmitOperator(
    task_id='process_taxi_stream',
    application='/opt/airflow/dags/consumer/target/scala-2.12/taxiconsumer_2.12-1.0.jar',
    conn_id='spark_default',
    dag=dag
)

check_kafka >> spark_job
