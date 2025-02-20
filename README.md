# Case de Engenharia de Dados  - EITS MLOps Engineering - Serasa Experian

1. Primeiro suba os serviços utilizando o docker compose file utilizando o terminal: **"docker compose up --build -d"**
2. Crie um tópico kafka utilizando executando esse comando: **docker exec -it nyc-taxi-case-kafka-1 sh -c "/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic nyc-taxi-rides --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"**
3. Após isso, **liste os tópicos no container do Kafka via docker** : **docker exec -it nyc-taxi-case-kafka-1 sh -c "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"**. Isso será interessante para validar que o tópico kafka que irá receber as mensagens está disponível.
4. Checado isso, vá até o diretório "**/nyc-taxi-fare-case/nyctaxi**" via terminal e execute o comando para startar o Producer **sbt "runMain fare.nyctaxi.producer.KafkaTaxiProducer"**
5. [Opicional] Caso você necessite checar os eventos do tópico kafka, só executar o comando no terminal: **docker exec -it nyc-taxi-case-kafka-1 sh -c "/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nyc-taxi-rides"**. Isso irá permitir que você consiga visualizar as novas mensagens enviadas.


## Comandos de execução e validação
- Para executar o producer: **sbt "runMain fare.nyctaxi.producer.KafkaTaxiProducer"**
- Para executar o consumer: **sbt "runMain fare.nyctaxi.consumer.TaxiConsumer"**


## 📌 Como fica a estrutura no Data Lake?
![img.png](img.png)

## 🚀 Vantagens dessa abordagem
✅ Consultas rápidas → Spark lê apenas os arquivos necessários
✅ Menos armazenamento desperdiçado → Evita pequenos arquivos dispersos
✅ Escalabilidade → Aguenta grandes volumes de dados sem travar
✅ Integração com batch e NRT → Pode rodar queries analíticas e streaming