package fare.nyctaxi.producer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import scala.util.{Try, Success, Failure}
import java.util.Properties

object KafkaTaxiProducer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Rides Kafka Producer")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"))
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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
      .csv("s3a://nyc-taxi-fare-prediction/data/train.csv")

    // ✅ 2. Prepare Kafka messages
    val kafkaMessages = df.select(
      col("key").cast(StringType).as("key"),
      to_json(struct(
        col("fare_amount").cast(DoubleType),
        col("pickup_datetime").cast(TimestampType),
        col("pickup_longitude").cast(DoubleType),
        col("pickup_latitude").cast(DoubleType),
        col("dropoff_longitude").cast(DoubleType),
        col("dropoff_latitude").cast(DoubleType),
        col("passenger_count").cast(IntegerType)
      )).as("value")
    ).filter(col("value").isNotNull) // Basic validation

    // ✅ 3. Simulate chunked streaming
    val chunkedDFs = kafkaMessages.randomSplit(Array.fill(10)(1.0)) // 10 chunks

    chunkedDFs.foreach { chunk =>
      if (!chunk.isEmpty) {
        chunk.write
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("topic", "nyc-taxi-rides")
          .save()

        Thread.sleep(1000) // Simulate delay between chunks
      }
    }

    spark.stop()
  }
}