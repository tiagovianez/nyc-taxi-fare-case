package fare.nyctaxi

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._

object Constants {
  private val config: Config = ConfigFactory.load("NycTaxiFare.conf") // Carrega o arquivo de configuração

  // 🗄️ Caminhos de fontes de dados
  val SOURCE_DATA_PATH: String = config.getString("Sources.inputData")
  val NEIGHBORHOOD_PATH: String = config.getString("Sources.neighborhoodPath")

  // 🏗️ Caminhos do Delta Lake
  val RAW_DELTA_PATH: String = config.getString("RawDelta.path")
  val CHECKPOINTS_PATH: String = config.getString("RawDelta.checkpoints")

  // 🎯 Caminho da camada Curated
  val CURATED_PARQUET_PATH: String = config.getString("CuratedParquet.out")


  // For Logs
  val log4jConfigPath = "src/main/resources/log4j.properties"


  // Kafka
  val kafkaBroker = "localhost:9092"
  val kafkaTopic = "nyc-taxi-rides"

  // Input Schemas

  val rawSchema = StructType(Array(
    StructField("key", StringType, nullable = false),
    StructField("fare_amount", FloatType, nullable = true),
    StructField("pickup_datetime", TimestampNTZType, nullable = true), // Novo tipo sem timezone
    StructField("pickup_longitude", DecimalType(8,6), nullable = true),
    StructField("pickup_latitude", DecimalType(8,6), nullable = true),
    StructField("dropoff_longitude", DecimalType(8,6), nullable = true),
    StructField("dropoff_latitude", DecimalType(8,6), nullable = true),
    StructField("passenger_count", ByteType, nullable = true), // `tinyint` em Spark é `ByteType`
    StructField("year", ShortType, nullable = true), // `smallint` no Spark é `ShortType`
    StructField("month", ByteType, nullable = true), // `tinyint`
    StructField("day", ByteType, nullable = true), // `tinyint`
    StructField("pickup_region", StringType, nullable = true)
  ))

  val neighborhoodSchema = StructType(Array(
    StructField("neighborhood", StringType, nullable = true),
    StructField("latitude", DecimalType(8,6), nullable = true),  // Reduz de DoubleType
    StructField("longitude", DecimalType(8,6), nullable = true)  // Reduz de DoubleType
  ))
}
