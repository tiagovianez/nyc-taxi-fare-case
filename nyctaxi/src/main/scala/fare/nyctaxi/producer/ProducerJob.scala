package fare.nyctaxi.producer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.log4j.{Level, Logger}
import fare.nyctaxi.Constants

object ProducerJob {
  def main(args: Array[String]): Unit = {

    System.setProperty("log4j.configuration", Constants.log4jConfigPath)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("NYC Taxi Rides Kafka Producer")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .format("parquet")
      .load(Constants.SOURCE_PARQUET_PATH)


    val kafkaMessages = df.select(
      col("key").cast(StringType).alias("key"),
      to_json(struct(
        col("fare_amount").cast(FloatType).alias("fare_amount"),
        col("pickup_datetime").cast(TimestampType).alias("pickup_datetime"),
        col("pickup_longitude").cast(DecimalType(8,6)).alias("pickup_longitude"),
        col("pickup_latitude").cast(DecimalType(8,6)).alias("pickup_latitude"),
        col("dropoff_longitude").cast(DecimalType(8,6)).alias("dropoff_longitude"),
        col("dropoff_latitude").cast(DecimalType(8,6)).alias("dropoff_latitude"),
        col("passenger_count").cast(ByteType).alias("passenger_count"),
        col("year").cast(ShortType).alias("year"),
        col("month").cast(ShortType).alias("month"),
        col("day").cast(ShortType).alias("day")
      )).alias("value")
    )

    val chunkedDFs = kafkaMessages.randomSplit(Array.fill(10)(0.01))

    chunkedDFs.foreach { chunk =>
      if (!chunk.isEmpty) {
        chunk.selectExpr("CAST(key AS STRING)", "value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "nyc-taxi-rides")
          .save()

        Thread.sleep(1000)
      }
    }

    spark.stop()
  }
}
