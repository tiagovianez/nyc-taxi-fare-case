package fare.nyctaxi.batch

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import fare.nyctaxi.Constants


object BatchTreatmentJob {
  def main(args: Array[String]): Unit = {

    System.setProperty("log4j.configuration", Constants.log4jConfigPath)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("io.delta").setLevel(Level.WARN)


    val spark = SparkSession.builder()
      .appName("BatchTransformationJob")
      .config("spark.master", "local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.executor.memory", "5g")
      .config("spark.driver.memory", "5g")
      .config("spark.sql.shuffle.partitions", "32")
      .config("spark.default.parallelism", "32")
      .config("spark.sql.files.maxPartitionBytes", "64MB")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
      .getOrCreate()

    import spark.implicits._

    val startTime = System.currentTimeMillis()

    val rawDF = spark.read
      .format("delta")
      .load(Constants.RAW_DELTA_PATH)
      .cache()

    val neighborhoodDF = spark.read
      .option("header", "true")
      .schema(Constants.neighborhoodSchema)
      .csv(Constants.NEIGHBORHOOD_PATH)
      .cache()


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
      .transform(rawDF)

    val pickupClusters = model.transform(pickupFeaturesDF)
      .select(
        col("key"),
        col("pickup_latitude"),
        col("pickup_longitude"),
        col("cluster").alias("pickup_cluster")
      )

    val finalDF = pickupClusters
      .join(clusteredNeighborhoods.withColumnRenamed("cluster", "pickup_cluster")
        .withColumnRenamed("neighborhood", "pickup_region"),
        Seq("pickup_cluster"), "left")
      .drop("pickup_cluster")

    val cleanedDF = rawDF
      .join(finalDF.select("key", "pickup_region"), Seq("key"), "left")
      .withColumnRenamed("neighborhood", "pickup_region")
      .withColumn("fare_amount", col("fare_amount").cast(FloatType))
      .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampNTZType))
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
      .coalesce(16)
      .write
      .format("delta")
      .mode("append")
      .partitionBy("year", "month", "day", "pickup_region")
      .save(Constants.CURATED_PARQUET_PATH)

    spark.stop()
  }
}
