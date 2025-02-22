package fare.nyctaxi.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object CSVToParquetConverter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CSV to Parquet Converter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 📌 Define o schema do CSV para evitar inferência automática
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

    // 📌 Caminho do CSV de entrada
    val inputCsvPath = "/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/source/train.csv"

    // 📌 Caminho de saída para Parquet
    val outputParquetPath = "/home/tiagovianez/projects/nyc-taxi-fare-case-source-data-lake/source_parquet"

    // ✅ Lê o CSV
    val df = spark.read
      .option("header", "true")       // CSV contém cabeçalho
      .option("inferSchema", "false") // Não inferir esquema automaticamente
      .option("delimiter", ",")       // Delimitador é vírgula
      .schema(schema)                 // Usa o esquema definido
      .csv(inputCsvPath)

    // ✅ Adiciona colunas para particionamento
    val dfPartitioned = df
      .withColumn("year", year($"pickup_datetime"))
      .withColumn("month", month($"pickup_datetime"))
      .withColumn("day", dayofmonth($"pickup_datetime"))

    // ✅ Escreve em Parquet com particionamento
    dfPartitioned
      .coalesce(4) // Ajusta o número de arquivos
      .write
      .mode("overwrite") // Sobrescreve se já existir
      .format("parquet")
      .partitionBy("year", "month", "day") // Particiona os dados
      .save(outputParquetPath)

    println(s"✅ Conversão concluída! Arquivos Parquet salvos em: $outputParquetPath")

    spark.stop()
  }
}
