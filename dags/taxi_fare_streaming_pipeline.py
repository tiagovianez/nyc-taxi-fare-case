import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from kafka.admin import KafkaAdminClient, NewTopic
from airflow.hooks.base import BaseHook



load_dotenv()

def check_csv_exists():
    if CSV_PATH is None:
        raise ValueError("CSV_PATH esta indefinido. Verifique o arquivo airflow.env e a configuração do docker.")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Arquivo {CSV_PATH} não encontrado dentro do container!")

    print(f"arquivo {CSV_PATH} encontrado dentro do container. continuando DAG...")

def create_kafka_topic_func():
    admin_client = KafkaAdminClient(bootstrap_servers="nyc-taxi-fare-case-kafka-1:9092")
    topic_name = "nyc-taxi-rides"
    topic_list = [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]

    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Tópico {topic_name} criado com sucesso!")
    else:
        print(f"Tópico {topic_name} já existe.")


def validate_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()
    if "nyc-taxi-rides" in topics:
        return True
    else:
        raise ValueError("Tópico Kafka não encontrado!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC TAXI Pipeline",
    schedule_interval="@daily",
    catchup=False,
)


create_kafka_topic = PythonOperator(
    task_id="create_kafka_topic",
    python_callable=create_kafka_topic_func,
    dag=dag,
)


validate_kafka_topic_task = PythonOperator(
    task_id='validate_kafka_topic',
    python_callable=validate_kafka_topic,
    dag=dag
)


wait_for_csv = PythonOperator(
    task_id="check_csv_exists",
    python_callable=check_csv_exists,
    execution_timeout=timedelta(seconds=15),
    dag=dag,
)


start_producer = BashOperator(
    task_id="start_producer",
    bash_command="spark-submit "
                 "--master spark://spark-master:7077 "
                 "/opt/bitnami/spark/jobs/nycTaxi-assembly-0.0.1.jar"
    ,
    execution_timeout=timedelta(seconds=30),
    dag=dag,
)


start_consumer = BashOperator(
    task_id="start_consumer",
    bash_command="spark-submit "
                 "--class fare.nyctaxi.jobs.ProducerJob "
                 "--master local[3] "
                 "--spark.sql.extensions 'io.delta.sql.DeltaSparkSessionExtension'"
                 "--spark.sql.catalog.spark_catalog 'org.apache.spark.sql.delta.catalog.DeltaCatalog'"
                 "/opt/airflow/jobs/nycTaxi-assembly-0.0.1.jar",
execution_timeout=timedelta(minutes=30),
dag=dag,
)


start_treatment = BashOperator(
    task_id="start_treatment",
    bash_command=f"spark-submit --class fare.nyctaxi.jobs.MainScript \
                 -- jars /home/tiagovianez/projects/nyc-taxi-case/nyctaxi/target/scala-2.12/ \
                 nycTaxi-assembly-0.0.1.jar --job treatment",
execution_timeout=timedelta(minutes=30),
dag=dag,
)


create_kafka_topic >> validate_kafka_topic_task >> wait_for_csv
wait_for_csv >> start_producer >> start_consumer >> start_treatment