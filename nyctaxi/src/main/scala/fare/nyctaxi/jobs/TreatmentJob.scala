package fare.nyctaxi.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import fare.nyctaxi.Constants
import org.apache.spark.sql.Row
import io.delta.tables._


object TreatmentJob {
  def main(args: Array[String]): Unit = {

    System.setProperty("log4j.configuration", Constants.log4jConfigPath)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("io.delta").setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("TreatmentJob")
      .config("spark.master", "local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.executor.memory", "5g")
      .config("spark.driver.memory", "5g")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.default.parallelism", "8")
      .config("spark.sql.streaming.checkpointLocation", Constants.CHECKPOINTS_CURATED_PATH + "/treatment")
      .getOrCreate()

    import spark.implicits._

    val rawDF = spark.read
      .format("delta")
      .load(Constants.RAW_DELTA_PATH)
      .filter(col("pickup_latitude").isNotNull && col("pickup_longitude").isNotNull)
      .withWatermark("pickup_datetime", "10 minutes")
      .limit(1000)

    val neighborhoodDF = spark.read
      .option("header", "true")
      .schema(Constants.neighborhoodSchema)
      .csv(Constants.NEIGHBORHOOD_PATH)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("latitude", "longitude"))
      .setOutputCol("features")

    val featureDF = vectorAssembler.transform(neighborhoodDF)

    val kmeans = new KMeans()
      .setK(12)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")


    val model = kmeans.fit(featureDF)

    val clusteredNeighborhoods = model.transform(featureDF)
      .select("neighborhood", "cluster")


    val pickupFeaturesDF = new VectorAssembler()
      .setInputCols(Array("pickup_latitude", "pickup_longitude"))
      .setOutputCol("features")
      .setHandleInvalid("skip")
      .transform(rawDF)

    val pickupClusters = model.transform(pickupFeaturesDF)
      .select(
        col("key"),
        col("pickup_latitude"),
        col("pickup_longitude"),
        col("pickup_datetime"),
        col("cluster").alias("pickup_cluster")
      )

    val finalDF = pickupClusters
      .join(
        broadcast(clusteredNeighborhoods)
          .withColumnRenamed("cluster", "pickup_cluster")
          .withColumnRenamed("neighborhood", "pickup_region"),
        Seq("pickup_cluster"),
        "left"
      )
      .drop("pickup_cluster")

    val cleanedDF = rawDF
      .drop("pickup_region")
      .alias("raw")
      .join(
        finalDF
          .selectExpr("key as final_key", "pickup_region", "pickup_datetime as final_pickup_datetime")
          .alias("final"),
        expr("""
      raw.key = final.final_key AND
      raw.pickup_datetime BETWEEN final.final_pickup_datetime - INTERVAL 10 MINUTES
                              AND final.final_pickup_datetime + INTERVAL 10 MINUTES
    """),
        "left"
      )
      .drop("final_key", "final_pickup_datetime")
      .withColumnRenamed("neighborhood", "pickup_region")
      .withColumn("fare_amount", col("fare_amount").cast(FloatType))
      .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType))
      .withColumn("pickup_longitude", col("pickup_longitude").cast(DecimalType(8,6)))
      .withColumn("pickup_latitude", col("pickup_latitude").cast(DecimalType(8,6)))
      .withColumn("dropoff_longitude", col("dropoff_longitude").cast(DecimalType(8,6)))
      .withColumn("dropoff_latitude", col("dropoff_latitude").cast(DecimalType(8,6)))
      .withColumn("passenger_count", col("passenger_count").cast(ByteType))
      .withColumn("year", col("year").cast(ShortType))
      .withColumn("month", col("month").cast(ByteType))
      .withColumn("day", col("day").cast(ByteType))
      .dropDuplicates()

    cleanedDF
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", Constants.CHECKPOINTS_CURATED_PATH + "/treatment")
      .option("path", Constants.CURATED_DELTA_PATH)
      .partitionBy("year", "month", "day", "pickup_region")
      .option("mergeSchema", "true")
      .start()
      .awaitTermination()
  }
}
