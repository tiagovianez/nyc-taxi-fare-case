package fare.nyctaxi.producer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties
import org.apache.log4j.{Level, Logger}

object KafkaTaxiProducer {

  def main(args: Array[String]): Unit = {

    val log4jConfigPath = "src/main/resources/log4j.properties"
    System.setProperty("log4j.configuration", log4jConfigPath)

    // Reduce more logs at console
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

    val kafkaBroker = "localhost:9092"
    val kafkaTopic = "nyc-taxi-rides"

    val spark = SparkSession.builder()
      .appName("NYC Taxi Rides Kafka Producer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ✅ 1. Load CSV from S3 with explicit schema
    val taxiSchema = StructType(Array(
      StructField("key", StringType, nullable = true),
      StructField("fare_amount", DoubleType, nullable = true),
      StructField("pickup_datetime", TimestampType, nullable = true),
      StructField("pickup_longitude", DoubleType, nullable = true),
      StructField("pickup_latitude", DoubleType, nullable = true),
      StructField("dropoff_longitude", DoubleType, nullable = true),
      StructField("dropoff_latitude", DoubleType, nullable = true),
      StructField("passenger_count", IntegerType, nullable = true)
    ))

    val df = spark.read
      .option("header", "true")
      .schema(taxiSchema)
      .csv("/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/source/train.csv")


    // ✅ 3. Prepare Kafka messages
    val kafkaMessages = df.select(
      col("key").cast(StringType).alias("key"),  // ✅ Mantém a key como string
      to_json(struct(
        col("fare_amount").cast(DoubleType).alias("fare_amount"),
        col("pickup_datetime").cast(TimestampType).alias("pickup_datetime"),
        col("pickup_longitude").cast(DoubleType).alias("pickup_longitude"),
        col("pickup_latitude").cast(DoubleType).alias("pickup_latitude"),
        col("dropoff_longitude").cast(DoubleType).alias("dropoff_longitude"),
        col("dropoff_latitude").cast(DoubleType).alias("dropoff_latitude"),
        col("passenger_count").cast(IntegerType).alias("passenger_count")
      )).alias("value") // ✅ O JSON vai como value no Kafka
    )


    val chunkedDFs = kafkaMessages.randomSplit(Array.fill(10)(1.0)) // 10 chunks

    chunkedDFs.foreach { chunk =>
      if (!chunk.isEmpty) {
        chunk.selectExpr("CAST(key AS STRING)", "value") // ✅ Explicita a key antes de salvar
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBroker)
          .option("topic", "nyc-taxi-rides")
          .save()

        Thread.sleep(1000) // Simula delay entre os batches
      }
    }

    spark.stop()
  }
}
