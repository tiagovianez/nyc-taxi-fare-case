# NYC Taxi Fare Case 
# Case de Engenharia de Dados  - EITS MLOps Engineering - Serasa Experian


## Introdução

Este projeto tem como objetivo construir um pipeline de dados em tempo real (*Near Real-Time - NRT*) para processar corridas de táxi da cidade de Nova York. Ele envolve a ingestão de eventos via **Apache Kafka**, processamento usando **Apache Spark com Scala**, e armazenamento em **Delta Lake**. O pipeline suporta tanto consultas **batch** quanto **streaming**, possibilitando análise de tarifas por tempo e local.

## Arquitetura

Abaixo está o fluxo do projeto:

![](docs/architecture.png)

1. **ProducerJob**: Envia eventos do arquivo `train.csv` para um tópico Kafka.
2. **ConsumerJob**: Consome eventos do Kafka, aplica transformações e escreve na camada **RAW**.
3. **TreatmentJob**: Enriquecimento dos dados via Machine Learning (**KMeans**) e escrita na camada **CURATED**.
4. **Airflow DAG**: Orquestra todas as etapas do pipeline.
5. **Delta Lake**: Gerencia a camada RAW e CURATED, permitindo consultas **batch e NRT**.

---

## **Tecnologias Utilizadas**

-  **Apache Kafka** – Streaming de eventos  
-  **Apache Spark** – Processamento de dados em Scala  
-  **Delta Lake** – Armazenamento otimizado para consultas  
-  **Apache Airflow** – Orquestração das tarefas  
-  **Docker & Docker Compose** – Deploy dos serviços  
-  **Scala & SBT** – Desenvolvimento do pipeline   
-  **Apache Spark MLlib** – Aplicação de **KMeans** para agrupamento de localizações

---

## **Configuração e Execução**

### 1. Subindo os serviços**sh
docker compose up --build -d

#### 2. Criando o tópico Kafka
docker exec -it nyc-taxi-fare-case-kafka-1 sh -c "/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic nyc-taxi-rides --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"

### 3. Validando o tópico Kafka
docker exec -it nyc-taxi-fare-case-kafka-1 sh -c "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"



## Execução dos Jobs na máquina local
### ProducerJob
sbt "runMain fare.nyctaxi.jobs.ProducerJob"

### ConsumerJob
sbt "runMain fare.nyctaxi.jobs.ConsumerJob"

### TreatmentJob
sbt "runMain fare.nyctaxi.jobs.TreatmentJob"


### Para verificar mensanges no Kafka
docker exec -it nyc-taxi-fare-case-kafka-1 sh -c "/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nyc-taxi-rides"


## Estrutura do Data Lake
Os dados são armazenados em Delta Lake, particionados por:

- year/month/day
- pickup_region (Região da coleta)

Isso melhora a eficiência e escalabilidade do armazenamento.


## Airflow Comandos
### Verificar logs: 
docker logs nyc-taxi-fare-case-airflow-1
docker restart airflow


## Testes unitários
Os testes validam cada etapa do pipeline.

### ConsumerJobTests
- Processamento correto de mensagens Kafka
- Transformações de schema
- Escrita correta no Delta Lake

Executar teste unitário:

**sbt "testOnly fare.nyctaxi.jobs.ConsumerJobTests"**


### TreatmentJobTests
- Testa KMeans para agrupamento de bairros
- Valida junção correta entre pickups e bairros
- Garante que os dados no DataFrame final estejam corretos

Executar teste unitário:

**sbt "testOnly fare.nyctaxi.jobs.TreatmentJobTests"**

## Vantagens da Arquitetura

**- Baixo custo de armazenamento (arquivos compactados em Delta)**
**- Escalabilidade (suporta alta ingestão via Kafka)**
**- Consultas otimizadas (Delta Lake reduz latência)**
**- Processamento híbrido (Batch + Streamin**


## Conclusão

Esse pipeline procura oferecer uma solução robusta para processamento de eventos de táxi 
em tempo real, permitindo análise detalhada dos dados. 
O uso de Kafka + Spark + Delta Lake proporciona alta escalabilidade e performance.