package fare.nyctaxi.consumer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQueryListener
import io.delta.tables._
import org.apache.spark.sql.streaming.Trigger

object TaxiConsumer {
  def main(args: Array[String]): Unit = {

    val log4jConfigPath = "src/main/resources/log4j.properties"
    System.setProperty("log4j.configuration", log4jConfigPath)

    // Reduce logs no console
//    Logger.getLogger("org").setLevel(Level.WARN)
//    Logger.getLogger("akka").setLevel(Level.WARN)
//    Logger.getLogger("kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql.execution.streaming").setLevel(Level.INFO)
    Logger.getLogger("io.delta").setLevel(Level.INFO)

    val rawDeltaPath = "/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/data-lake/raw/"
    val checkpointLocation = "/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/checkpoints/raw/"

    val kafkaBroker = "localhost:9092"
    val kafkaTopic = "nyc-taxi-rides"

    val spark = SparkSession.builder
      .appName("TaxiStreamProcessor")
      .config("spark.master", "local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._


    val schema = new StructType()
      .add("fare_amount", DoubleType)
      .add("pickup_datetime", StringType)  // Vamos transformar depois em Timestamp
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
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as json_value"
      )
      .select(from_json($"json_value", schema).as("data"), $"key")
      .select("key", "data.*")
      .withColumn("pickup_datetime", to_timestamp($"pickup_datetime")) // Converte datetime para TimestampType
      .withColumn("year", year($"pickup_datetime"))
      .withColumn("month", month($"pickup_datetime"))
      .withColumn("day", dayofmonth($"pickup_datetime"))

    // ✅ Escrevendo no Delta Lake (particionado por year, month, day)
//    parsedDF
//      .writeStream
//      .format("delta")
//      .outputMode("append")
//      .option("checkpointLocation", "/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/checkpoints/raw")
//      .option("path", "/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/data-lake/raw")
//      .partitionBy("year", "month", "day") // 🚀 Particionamento eficiente!
//      .start()
//      .awaitTermination()

    parsedDF
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchCount = batchDF.count()  // Conta quantos registros estão indo para o Delta Lake

        println(s"\n📌 [INFO] - Batch ID: $batchId | Registros processados: $batchCount")
        println(s"📀 [INFO] - Iniciando escrita no Delta Lake (Raw Layer)...")

        batchDF
          .write
          .format("delta")
          .mode("append")
          .partitionBy("year", "month", "day") // 🚀 Particionamento eficiente!
          .save(rawDeltaPath)

        println(s"✅ [SUCESSO] - Batch $batchId armazenado com sucesso no Delta Lake!")
      }
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()
  }
}
