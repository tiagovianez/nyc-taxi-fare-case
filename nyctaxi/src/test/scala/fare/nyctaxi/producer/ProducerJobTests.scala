package fare.nyctaxi.producer

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProducerJobTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _ // Não usar implicit

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Test - NYC Taxi Rides Kafka Producer")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

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

  "ProducerJob" should "load CSV data correctly into a DataFrame" in {
    val localSpark = spark

    val data = Seq(
      ("1", 12.5, "2024-02-21 10:30:00", -73.987, 40.743, -73.985, 40.745, 2),
      ("2", 25.0, "2024-02-21 11:00:00", -73.982, 40.750, -73.980, 40.752, 1)
    )

    val df = data.toDF(
      "key", "fare_amount", "pickup_datetime", "pickup_longitude", "pickup_latitude",
      "dropoff_longitude", "dropoff_latitude", "passenger_count"
    )

    df.schema shouldBe taxiSchema
    df.count() shouldBe 2
  }

  it should "transform data into the correct Kafka message format" in {
    val localSpark = spark
    import localSpark.implicits._

    val df = Seq(
      ("1", 12.5, "2024-02-21 10:30:00", -73.987, 40.743, -73.985, 40.745, 2)
    ).toDF("key", "fare_amount", "pickup_datetime", "pickup_longitude", "pickup_latitude",
      "dropoff_longitude", "dropoff_latitude", "passenger_count")

    val kafkaMessages = df.select(
      col("key").cast(StringType).alias("key"),
      to_json(struct(
        col("fare_amount").cast(DoubleType).alias("fare_amount"),
        col("pickup_datetime").cast(TimestampType).alias("pickup_datetime"),
        col("pickup_longitude").cast(DoubleType).alias("pickup_longitude"),
        col("pickup_latitude").cast(DoubleType).alias("pickup_latitude"),
        col("dropoff_longitude").cast(DoubleType).alias("dropoff_longitude"),
        col("dropoff_latitude").cast(DoubleType).alias("dropoff_latitude"),
        col("passenger_count").cast(IntegerType).alias("passenger_count")
      )).alias("value")
    )

    kafkaMessages.schema.fieldNames should contain allElementsOf Array("key", "value")
    kafkaMessages.count() shouldBe 1
  }

  it should "split DataFrame into smaller chunks correctly" in {
    val localSpark = spark
    import localSpark.implicits._

    val df = Seq(
      ("1", "value1"),
      ("2", "value2"),
      ("3", "value3"),
      ("4", "value4"),
      ("5", "value5")
    ).toDF("key", "value")

    val chunkedDFs = df.randomSplit(Array.fill(10)(0.01))
    val totalRows = chunkedDFs.map(_.count()).sum

    totalRows shouldBe df.count()
  }
}
