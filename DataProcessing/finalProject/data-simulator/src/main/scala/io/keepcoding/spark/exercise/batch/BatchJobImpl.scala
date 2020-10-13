package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BatchJobImpl extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    println(filterDate.getYear, filterDate.getMonthValue, filterDate.getDayOfMonth, filterDate.getHour)
    spark
      .read
      .format("parquet")
      .option("path",s"${storagePath}/data")
      .load()
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {

    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichTelemetryWithMetadata(storageDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    storageDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }


  override def computeBytesHourly(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"value", $"type")
      //.withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withWatermark("timestamp","30 seconds")
      .groupBy($"id",window($"timestamp", "2 minutes"))  //deberia ser 1h
      .agg(sum($"value").as("value"))
      .withColumn("type", lit("user_total_bytes"))
      .select( $"window.start".as("timestamp"), $"id", $"value", $"type")

  }



  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  def main(args: Array[String]): Unit = run(args)
}
