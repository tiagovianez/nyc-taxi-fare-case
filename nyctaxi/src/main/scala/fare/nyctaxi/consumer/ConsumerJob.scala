package fare.nyctaxi.consumer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import io.delta.tables._
import fare.nyctaxi.Constants

object ConsumerJob {
  def main(args: Array[String]): Unit = {

    System.setProperty("log4j.configuration", Constants.log4jConfigPath)

    Logger.getLogger("org.apache.spark.sql.execution.streaming").setLevel(Level.INFO)
    Logger.getLogger("io.delta").setLevel(Level.INFO)

    val spark = SparkSession.builder
      .appName("TaxiStreamProcessor")
      .config("spark.master", "local[3]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    val rawKafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Constants.kafkaBroker)
      .option("subscribe", Constants.kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val parsedDF = rawKafkaDF
      .selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as json_value"
      )
      .select(from_json($"json_value", Constants.rawSchema).as("data"), $"key")
      .select("key", "data.*")
      .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampNTZType))
      .withColumn("year", year($"pickup_datetime").cast(ShortType))
      .withColumn("month", month($"pickup_datetime").cast(ByteType))
      .withColumn("day", dayofmonth($"pickup_datetime").cast(ByteType))

    parsedDF
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchCount = batchDF.count()

        batchDF
          .write
          .format("delta")
          .mode("append")
          .partitionBy("year", "month", "day")
          .save(Constants.RAW_DELTA_PATH)
      }
      .option("checkpointLocation", Constants.CHECKPOINTS_PATH)
      .start()
      .awaitTermination()
  }
}
