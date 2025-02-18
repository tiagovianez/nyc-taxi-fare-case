package nyc.taxi.consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object TaxiConsumer {
  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "localhost:9092")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "nyc-taxi-rides")
    val deltaLakePath = sys.env.getOrElse("DELTA_LAKE_PATH", "/datalake/raw/nyc_taxi_facts")

    val spark = SparkSession.builder
      .appName("TaxiStreamProcessor")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()

    import spark.implicits._

    // Define schema to match Kafka producer messages
    val schema = new StructType()
      .add("key", StringType)
      .add("fare_amount", DoubleType)
      .add("pickup_datetime", StringType)  // Keep as String, convert to Timestamp later
      .add("pickup_longitude", DoubleType)
      .add("pickup_latitude", DoubleType)
      .add("dropoff_longitude", DoubleType)
      .add("dropoff_latitude", DoubleType)
      .add("passenger_count", IntegerType)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Deserialize JSON and parse into structured format
    val parsedDF = df
      .selectExpr("CAST(value AS STRING) as json_data")
      .select(from_json(col("json_data"), schema).as("data"))
      .select("data.*")
      .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss 'UTC'"))

    // Apply processing: group by time and passenger count
    val resultDF = parsedDF
      .withWatermark("pickup_datetime", "10 minutes")
      .groupBy(window($"pickup_datetime", "10 minutes"), $"passenger_count")
      .agg(
        avg($"fare_amount").as("avg_fare"),
        count("*").as("num_rides")
      )

    // Write to Delta Lake
    resultDF
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("path", deltaLakePath)
      .option("checkpointLocation", "/datalake/checkpoints/taxi_fare")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }
}
