package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
      .persist()
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


  override def computeTotalBytes(storageDF: DataFrame, aggColumn: String, filterDate: OffsetDateTime): DataFrame = {

    import storageDF.sparkSession.implicits._
    storageDF
      .select(col(aggColumn).as("id"), $"bytes")
      .groupBy($"id")
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit(s"${aggColumn}_total_bytes"))
      .withColumn("timestamp", lit(filterDate.toEpochSecond).cast(TimestampType))
  }

  override def computeQuotaExceeded(userBytes: DataFrame, metadataDF: DataFrame): DataFrame = {
    userBytes.select($"id", $"value", $"timestamp").as("user")
      .join(
        metadataDF.select($"email", $"id", $"quota").as("metadata"),
        $"user.id" === $"metadata.id" && $"user.value" > $"metadata.quota"
      )
      .select($"metadata.email".as("email"), $"user.value".as("usage"), $"metadata.quota".as("quota"), $"user.timestamp".as("timestamp"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
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

  def main(args: Array[String]): Unit = run(args)
}
