import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.getenv("PROJECT_ROOT")
JAR_NAME = os.getenv("JAR_NAME")
JAR_PATH = f"{PROJECT_ROOT}/target/scala-2.12/{JAR_NAME}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "nyc_taxi_pipeline",
    default_args=default_args,
    description="Orquestração dos jobs no Airflow via .jar",
    schedule_interval="0 3 * * *",  # Executa diariamente às 3AM
    catchup=False,
)


wait_for_csv = FileSensor(
    task_id="wait_for_csv",
    filepath=f"{PROJECT_ROOT}/source_csv/*.csv",
    poke_interval=60,
    timeout=600,
    mode="poke",
    dag=dag,
)


csv_to_parquet = BashOperator(
    task_id="csv_to_parquet",
    bash_command=f"spark-submit --class fare.nyctaxi.jobs.CSVToParquetConverter {JAR_PATH}",
    dag=dag,
)


wait_for_parquet = FileSensor(
    task_id="wait_for_parquet",
    filepath=f"{PROJECT_ROOT}/source_parquet/",
    poke_interval=60,
    timeout=600,
    mode="poke",
    dag=dag,
)


start_producer = BashOperator(
    task_id="start_producer",
    bash_command=f"spark-submit --class fare.nyctaxi.producer.ProducerJob {JAR_PATH}",
    dag=dag,
)


start_consumer = BashOperator(
    task_id="start_consumer",
    bash_command=f"spark-submit --class fare.nyctaxi.consumer.ConsumerJob {JAR_PATH}",
    dag=dag,
)


start_treatment = BashOperator(
    task_id="start_treatment",
    bash_command=f"spark-submit --class fare.nyctaxi.treatment.TreatmentJob {JAR_PATH}",
    dag=dag,
)


wait_for_csv >> csv_to_parquet >> wait_for_parquet
wait_for_parquet >> start_producer >> start_consumer >> start_treatment
