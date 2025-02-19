package fare.nyctaxi.consumer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQueryListener

object TaxiConsumer {
  def main(args: Array[String]): Unit = {

    val log4jConfigPath = "src/main/resources/log4j.properties"
    System.setProperty("log4j.configuration", log4jConfigPath)

    // Reduce more logs at console
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)


    val kafkaBroker = "localhost:9092"
    val kafkaTopic = "nyc-taxi-rides"

    val spark = SparkSession.builder
      .appName("TaxiStreamProcessor")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._

    // 📌 Define o schema para o JSON recebido do Kafka
    val schema = new StructType()
      .add("fare_amount", DoubleType)
      .add("pickup_datetime", StringType)
      .add("pickup_longitude", DoubleType)
      .add("pickup_latitude", DoubleType)
      .add("dropoff_longitude", DoubleType)
      .add("dropoff_latitude", DoubleType)
      .add("passenger_count", IntegerType)


    // 🔥 Leitura dos dados do Kafka
    val rawKafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // 📌 Conversão de JSON para DataFrame
    val parsedDF = rawKafkaDF
      .selectExpr(
        "CAST(key AS STRING) as key", // ✅ Agora pegamos a key corretamente
        "CAST(value AS STRING) as json_value"
      )
      .select(from_json($"json_value", schema).as("data"), $"key") // ✅ Inclui a key no DataFrame final
      .select("key", "data.*") // ✅ Expande todas as colunas corretamente (sem duplicar key!)


    parsedDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"\n🔎 BATCH RECEBIDO -> ID: $batchId")
        batchDF.show(10, false) // Mostra 10 mensagens sem truncar
      }
      .start()
      .awaitTermination()

    // ✅ Escreve os dados no console para visualização
//    val queryConsole = parsedDF
//      .coalesce(1) // 🔥 Força apenas 1 partição para desacelerar a exibição
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .option("truncate", "false")
//      .option("numRows", 20) // ✅ Limita para 20 linhas por batch
//      .start()
//
//    // ✅ Salva os dados brutos em JSON para depuração posterior
//    val queryFile = rawKafkaDF
//      .selectExpr("CAST(value AS STRING) as json_value")
//      .writeStream
//      .format("json")
//      .option("path", "/tmp/kafka_raw_data")
//      .option("checkpointLocation", "/tmp/kafka_checkpoints")
//      .outputMode("append")
//      .start()
//
//    // 🚀 Aguarda a execução das queries
//    queryConsole.awaitTermination()
//    queryFile.awaitTermination()
  }
}
