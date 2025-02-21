from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 5
}

dag = DAG(
    'nyc_taxi_batch_processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

run_spark_job = SparkSubmitOperator(
    task_id='run_batch_job',
    application='/home/tiagovianez/projects/nyc-taxi-fare-case/nyctaxi/target/scala-2.12/nycTaxi-assembly-0.0.1.jar',
    java_class='fare.nyctaxi.jobs.MainScript',
    application_args=['--job', 'batch'],
    conn_id='spark_default',
    dag=dag
)

run_spark_job
