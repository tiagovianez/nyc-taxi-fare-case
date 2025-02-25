package fare.nyctaxi.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.delta.tables._
import org.apache.spark.sql.streaming.Trigger

class ConsumerJobTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Test - Consumer Job")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  val rawSchema = StructType(Array(
    StructField("key", StringType, nullable = true),
    StructField("fare_amount", DoubleType, nullable = true),
    StructField("pickup_datetime", TimestampType, nullable = true),
    StructField("pickup_longitude", DoubleType, nullable = true),
    StructField("pickup_latitude", DoubleType, nullable = true),
    StructField("dropoff_longitude", DoubleType, nullable = true),
    StructField("dropoff_latitude", DoubleType, nullable = true),
    StructField("passenger_count", IntegerType, nullable = true)
  ))

  "ConsumerJob" should "parse Kafka JSON messages correctly" in {
    import spark.implicits._

    val inputData = Seq(
      """{"key": "1", "fare_amount": 12.5, "pickup_datetime": "2024-02-21T10:30:00",
         "pickup_longitude": -73.987, "pickup_latitude": 40.743,
         "dropoff_longitude": -73.985, "dropoff_latitude": 40.745, "passenger_count": 2}""",
      """{"key": "2", "fare_amount": 25.0, "pickup_datetime": "2024-02-21T11:00:00",
         "pickup_longitude": -73.982, "pickup_latitude": 40.750,
         "dropoff_longitude": -73.980, "dropoff_latitude": 40.752, "passenger_count": 1}"""
    ).toDF("json_value")

    val parsedDF = inputData
      .select(from_json($"json_value", rawSchema).as("data"))
      .select("data.*")

    parsedDF.schema shouldBe rawSchema
    parsedDF.count() shouldBe 2
  }

  it should "apply transformations correctly" in {
    import spark.implicits._

    val df = Seq(
      ("1", 12.5, "2024-02-21T10:30:00", -73.987, 40.743, -73.985, 40.745, 2)
    ).toDF("key", "fare_amount", "pickup_datetime", "pickup_longitude", "pickup_latitude",
      "dropoff_longitude", "dropoff_latitude", "passenger_count")

    val transformedDF = df
      .withColumn("fare_amount", col("fare_amount").cast(FloatType))
      .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType))
      .withColumn("year", year($"pickup_datetime").cast(ShortType))
      .withColumn("month", month($"pickup_datetime").cast(ByteType))
      .withColumn("day", dayofmonth($"pickup_datetime").cast(ByteType))

    transformedDF.schema.fieldNames should contain allElementsOf Array(
      "fare_amount", "pickup_datetime", "year", "month", "day"
    )

    transformedDF.select("year").collect().head.getShort(0) shouldBe 2024
    transformedDF.count() shouldBe 1
  }

  it should "write data to Delta Lake correctly" in {
    import spark.implicits._

    val df = Seq(
      ("1", 12.5, "2024-02-21T10:30:00", -73.987, 40.743, -73.985, 40.745, 2)
    ).toDF("key", "fare_amount", "pickup_datetime", "pickup_longitude", "pickup_latitude",
      "dropoff_longitude", "dropoff_latitude", "passenger_count")

    val deltaPath = "tmp/delta-test"

    df
      .write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    val deltaTable = spark.read.format("delta").load(deltaPath)

    deltaTable.count() shouldBe 1
  }
}
