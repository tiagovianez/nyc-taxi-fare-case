package fare.nyctaxi

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._

object Constants {
  private val config: Config = ConfigFactory.load("NycTaxiFare.conf") // Carrega o arquivo de configuração

  val SOURCE_CSV_PATH: String = config.getString("Sources.inputCsvData")
  val SOURCE_PARQUET_PATH: String = config.getString("Sources.inputParquetData")
  val NEIGHBORHOOD_PATH: String = config.getString("Sources.neighborhoodPath")

  val RAW_DELTA_PATH: String = config.getString("RawDelta.path")
  val CHECKPOINTS_RAW_PATH: String = config.getString("RawDelta.checkpointsRaw")

  val CURATED_DELTA_PATH: String = config.getString("CuratedDelta.out")
  val CHECKPOINTS_CURATED_PATH: String = config.getString("CuratedDelta.checkpointsCurated")


  val log4jConfigPath = "src/main/resources/log4j.properties"

  val kafkaBroker = "localhost:9092"
  val kafkaTopic = "nyc-taxi-rides"


  val taxiSchema = StructType(Array(
    StructField("key", StringType, nullable = true),
    StructField("fare_amount", FloatType, nullable = true),
    StructField("pickup_datetime", TimestampType, nullable = true),
    StructField("pickup_longitude", DoubleType, nullable = true),
    StructField("pickup_latitude", DoubleType, nullable = true),
    StructField("dropoff_longitude", DoubleType, nullable = true),
    StructField("dropoff_latitude", DoubleType, nullable = true),
    StructField("passenger_count", ByteType, nullable = true)
  ))

  val rawSchema = StructType(Array(
    StructField("key", StringType, nullable = false),
    StructField("fare_amount", FloatType, nullable = true),
    StructField("pickup_datetime", TimestampType, nullable = true),
    StructField("pickup_longitude", DoubleType, nullable = true),
    StructField("pickup_latitude", DoubleType, nullable = true),
    StructField("dropoff_longitude", DoubleType, nullable = true),
    StructField("dropoff_latitude", DoubleType, nullable = true),
    StructField("passenger_count", ByteType, nullable = true),
    StructField("year", ShortType, nullable = true),
    StructField("month", ByteType, nullable = true),
    StructField("day", ByteType, nullable = true),
    StructField("pickup_region", StringType, nullable = true)
  ))

  val neighborhoodSchema = StructType(Array(
    StructField("neighborhood", StringType, nullable = true),
    StructField("latitude", DecimalType(8,6), nullable = true),
    StructField("longitude", DecimalType(8,6), nullable = true)
  ))


}
