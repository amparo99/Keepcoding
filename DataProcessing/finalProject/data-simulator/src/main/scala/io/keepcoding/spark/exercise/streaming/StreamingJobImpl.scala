package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future


object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Final exercise")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaServer)
      .option("subscribe",topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val schema = StructType(Seq(
      StructField("timestamp", LongType,nullable = false),
      StructField("id", StringType,nullable = false),
      StructField("antenna_id", StringType,nullable = false),
      StructField("bytes", LongType,nullable = false),
      StructField("app", StringType,nullable = false)
    ))

    dataFrame
      .select(from_json(col("value").cast(StringType),schema).as("json"))
      .select("json.*")

  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {

    spark
      .read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()

  }

  override def enrichAntennaWithMetadata(telemetryDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    telemetryDF.as("telemetry")
      .join(
        metadataDF.as("metadata"),
        $"telemetry.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  override def computeTotalBytesAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withWatermark("timestamp","30 seconds")
      .groupBy($"antenna_id",window($"timestamp", "1 minutes")) //aqui deberian ser 5 min, pero para que vaya mas rapido
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("antenna_total_bytes"))
      .select( $"window.start".as("timestamp"), $"telemetry.antenna_id".as("id"), $"value", $"type")

  }

  override def computeTotalBytesUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withWatermark("timestamp","30 seconds")
      .groupBy($"id",window($"timestamp", "1 minutes")) //aqui deberian ser 5 min, pero para que vaya mas rapido
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("user_total_bytes"))
      .select( $"window.start".as("timestamp"), $"telemetry.id".as("id"), $"value", $"type")

  }


  override def computeTotalBytesApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withWatermark("timestamp","30 seconds")
      .groupBy($"app",window($"timestamp", "1 minutes")) //aqui deberian ser 5 min, pero para que vaya mas rapido
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("app_total_bytes"))
      .select( $"window.start".as("timestamp"), $"telemetry.app".as("id"), $"value", $"type")
  }



  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
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
      .start()
      .awaitTermination()

  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future{
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year","month","day","hour")
      .format("parquet")
      .option("path",s"$storageRootPath/data")
      .option("checkpointLocation",s"$storageRootPath/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)
}
