# Case de Engenharia de Dados  - EITS MLOps Engineering - Serasa Experian




## Para ativar variável de ambiente:
source venv/bin/activate


1. Primeiro suba os serviços utilizando o docker compose file utilizando o terminal: **"docker compose up --build -d"**
2. Crie um tópico kafka utilizando executando esse comando: **docker exec -it kafka sh -c "/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic nyc-taxi-rides --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"**
3. Após isso, liste os tópicos no container do Kafka via docker : **docker exec -it kafka sh -c "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"**. Isso será interessante para validar que o tópico kafka que irá receber as mensagens está disponível.
4. Checado isso, vá até o diretório "**/nyc-taxi-fare-case/nyctaxi**" via terminal e execute o comando para startar o Producer **sbt "runMain fare.nyctaxi.producer.ProducerJob"**
5. [Opicional] Caso você necessite checar os eventos do tópico kafka, só executar o comando no terminal: **docker exec -it kafka sh -c "/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nyc-taxi-rides"**. Isso irá permitir que você consiga visualizar as novas mensagens enviadas.


## Comandos de execução e validação
- Para executar o producer: **sbt "runMain fare.nyctaxi.producer.KafkaTaxiProducer"**
- Para executar o consumer: **sbt "runMain fare.nyctaxi.consumer.TaxiConsumer"**


## Validações dentro do container do Kafka
- Primeiro, valide que todos os containers necessários estão de pé: **docker ps**
- Acesso o container do Kafka utilizando o comando: **docker exec -it kafka bash**
- Uma vez dentro do container do Kafka já será possível realizar algumas validações do tipo:
- Liste todos os tópicos no Kafka: **kafka-topics.sh --bootstrap-server localhost:9092 --list**
- Caso não exista nenhum tópico e você deseja criá-lo, só executar: **kafka-topics.sh --bootstrap-server localhost:9092 --create --topic nyc-taxi-rides --partitions 3 --replication-factor 1**
- Para testar a conexão do Kafka com o Producer




## 📌 Como fica a estrutura no Data Lake?
![img.png](img.png)

## 🚀 Vantagens dessa abordagem
✅ Consultas rápidas → Spark lê apenas os arquivos necessários
✅ Menos armazenamento desperdiçado → Evita pequenos arquivos dispersos
✅ Escalabilidade → Aguenta grandes volumes de dados sem travar
✅ Integração com batch e NRT → Pode rodar queries analíticas e streaming




## Analysys memory

### amout of data stored in the raw: 354.8MB
### amount of data stored in the curated: 457.8MB



## Airfllow comandos
🔹 Comandos Airflow

**airflow db upgrade** → Atualiza o banco de dados.
**airflow users create** ... → Cria um usuário administrador.
**airflow scheduler** & **airflow webserver** → Inicia os serviços do Airflow.
**docker logs -f nyc-taxi-fare-case-airflow-1** -> Verifica as logs do airflow


## Comandos do docker
**docker volume ls** -> Verifica se ainda tem volume ativo
**docker volume rm <service_data>** -> remove manualmente o volume do serviço




## SBT configurações:

1. Configurar SBT_OPTS para aumentar a memória

Antes de rodar o sbt assembly, execute o seguinte comando no terminal para aumentar a memória disponível:

export SBT_OPTS="-Xms2G -Xmx4G -XX:MaxMetaspaceSize=1G -XX:+UseG1GC"


## Como o Delta Lake lida com Escritas em Streaming?

O Delta Lake foi projetado para suportar leitura e escrita simultâneas, garantindo Atomicidade, Consistência, Isolamento e Durabilidade (ACID). Isso significa que:

✅ Nenhum analista ou cientista de dados verá dados incompletos (porque o Delta faz commit de transações atomizadas).
✅ Os dados sempre estarão disponíveis para leitura sem conflitos.
✅ Leitores e Escritores podem trabalhar ao mesmo tempo, pois o Delta usa arquivos transacionais para controle de versão.


### Para ler tabelas em Delta utilizando o Spark/Scala local da máquina:
Execute esse comando: spark-shell --packages io.delta:delta-core_2.12:2.4.0




### Como Melhorar a Eficiência?

Para garantir que ninguém tenha impacto negativo na performance, algumas boas práticas podem ser aplicadas:
1️⃣ Ativar Auto-Optimize (Compactação)

Como o streaming grava muitos pequenos arquivos, ativar Auto-Optimize e Auto-Compaction no Delta ajuda a evitar fragmentação:

ALTER TABLE delta.`/path/to/curated` SET TBLPROPERTIES (
'delta.autoOptimize.optimizeWrite' = 'true',
'delta.autoOptimize.autoCompact' = 'true'
);

**Benefício**:
Reduz a fragmentação e melhora a leitura sem afetar o streaming.

###  Conclusão

✅ Sim, a leitura pode acontecer sem impacto para analistas e cientistas de dados.
✅ Delta Lake garante consistência dos dados mesmo com escrita em streaming.
✅ Ativar auto-compactação melhora a performance e reduz fragmentação.
✅ Materialized Views podem desacoplar leitura e escrita.

Se você seguir essas práticas, seu pipeline será sólido e escalável, garantindo que ninguém seja impactado pela escrita contínua! 🚀🔥




## Docker do Airflow
### Ps.: Se certifique que você esteja no diretório do docker
Para subir os serviços do airflow via docker: **docker compose -f docker-compose.Airflow.yml up -d --build**


#### Reinicialização do airflow via docker: 
**docker compose -f docker-compose.Airflow.yml down
docker compose -f docker-compose.Airflow.yml up -d**



### Para limpar espaço no docker:
**docker system prune -a --volumes**

