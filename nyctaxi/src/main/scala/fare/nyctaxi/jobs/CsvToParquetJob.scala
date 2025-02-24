package fare.nyctaxi.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import fare.nyctaxi.Constants

object CsvToParquetJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CSV to Parquet Converter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val schema = StructType(Array(
      StructField("key", StringType, nullable = true),
      StructField("fare_amount", FloatType, nullable = true),
      StructField("pickup_datetime", TimestampType, nullable = true),
      StructField("pickup_longitude", DoubleType, nullable = true),
      StructField("pickup_latitude", DoubleType, nullable = true),
      StructField("dropoff_longitude", DoubleType, nullable = true),
      StructField("dropoff_latitude", DoubleType, nullable = true),
      StructField("passenger_count", IntegerType, nullable = true)
    ))

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .option("delimiter", ",")
      .schema(schema)
      .csv(Constants.SOURCE_CSV_PATH)


    val dfPartitioned = df
      .withColumn("year", year($"pickup_datetime"))
      .withColumn("month", month($"pickup_datetime"))
      .withColumn("day", dayofmonth($"pickup_datetime"))

    dfPartitioned
      .coalesce(4)
      .write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save(Constants.SOURCE_PARQUET_PATH)

    spark.stop()
  }
}
