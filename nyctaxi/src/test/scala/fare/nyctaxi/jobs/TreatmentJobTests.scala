package fare.nyctaxi.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class TreatmentJobTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("Test - Treatment Job")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/treatment_test_checkpoint")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }


  val rawSchema = StructType(Array(
    StructField("key", StringType, nullable = false),
    StructField("fare_amount", DoubleType, nullable = false),
    StructField("pickup_datetime", TimestampType, nullable = false),
    StructField("pickup_longitude", DoubleType, nullable = false),
    StructField("pickup_latitude", DoubleType, nullable = false),
    StructField("dropoff_longitude", DoubleType, nullable = false),
    StructField("dropoff_latitude", DoubleType, nullable = false),
    StructField("passenger_count", IntegerType, nullable = false)
  ))

  "TreatmentJob" should "apply KMeans clustering correctly to neighborhoods" in {


    val neighborhoodData = Seq(
      ("Manhattan", 40.7831, -73.9712),
      ("Brooklyn", 40.6782, -73.9442)
    ).toDF("neighborhood", "latitude", "longitude")


    val pickupData = Seq(
      ("1", 10.5, "2024-02-21 10:30:00", -73.985, 40.748),
      ("2", 15.0, "2024-02-21 11:00:00", -73.982, 40.750)
    ).toDF("key", "fare_amount", "pickup_datetime", "pickup_longitude", "pickup_latitude")

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("latitude", "longitude"))
      .setOutputCol("features")

    val featureDF = vectorAssembler.transform(neighborhoodData)

    val kmeans = new KMeans()
      .setK(2)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(featureDF)

    val clusteredNeighborhoods = model.transform(featureDF)
      .select("neighborhood", "cluster")

    clusteredNeighborhoods.count() shouldBe 2
  }
}
