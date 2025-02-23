import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

load_dotenv()

# Definições de ambiente
PROJECT_ROOT = os.getenv("PROJECT_ROOT")
SCOPT_PATH = os.getenv("SCOPT_PATH")
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


### 🔹 SENSOR: Espera arquivos CSV na pasta fonte
wait_for_csv = FileSensor(
    task_id="wait_for_csv",
    filepath=f"{PROJECT_ROOT}/source_csv/*.csv",
    poke_interval=60,
    timeout=1800,  # Espera até 30 min
    mode="poke",
    dag=dag,
)

### 🔹 CONVERTER CSV PARA PARQUET
csv_to_parquet = BashOperator(
    task_id="csv_to_parquet",
    bash_command=f"spark-submit --class fare.nyctaxi.jobs.MainScript {JAR_PATH} csvtoparquet",
    execution_timeout=1800,  # Timeout de 30 min
    dag=dag,
)

### 🔹 SENSOR: Espera a geração do parquet
wait_for_parquet = FileSensor(
    task_id="wait_for_parquet",
    filepath=f"{PROJECT_ROOT}/source_parquet/*.parquet",
    poke_interval=60,
    timeout=1800,  # Espera até 30 min
    mode="poke",
    dag=dag,
)

### 🔹 PRODUTOR: Envia eventos para o Kafka
start_producer = BashOperator(
    task_id="start_producer",
    bash_command=f"spark-submit --class fare.nyctaxi.jobs.MainScript \
  --jars /home/tiagovianez/.ivy2/cache/com.github.scopt/scopt_2.12/jars/scopt_2.12-4.1.0.jar \
  nycTaxi-assembly-0.0.1.jar --job producer",
    execution_timeout=1800,
    dag=dag,
)

### 🔹 CONSUMIDOR: Consome eventos e grava em RAW Delta
start_consumer = BashOperator(
    task_id="start_consumer",
    bash_command=f"spark-submit --class fare.nyctaxi.jobs.MainScript {JAR_PATH} consumer",
    execution_timeout=1800,
    dag=dag,
)

### 🔹 TRATAMENTO: Processa e grava na camada CURATED
start_treatment = BashOperator(
    task_id="start_treatment",
    bash_command=f"spark-submit --class fare.nyctaxi.jobs.MainScript {JAR_PATH} treatment",
    execution_timeout=1800,
    dag=dag,
)

# Definição das dependências entre as tasks
wait_for_csv >> csv_to_parquet >> wait_for_parquet
wait_for_parquet >> start_producer >> start_consumer >> start_treatment
