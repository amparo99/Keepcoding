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
    .appName("StreamingJobImpl")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaServer)
      .option("subscribe",topic)
      .option("startingOffsets", "earliest")
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
      .select(from_json($"value".cast(StringType),schema).as("json"))
      .select("json.*")

  }

  override def computeTotalBytes(dataFrame: DataFrame, aggColumn: String): DataFrame = {
    dataFrame
      .select($"timestamp", col(aggColumn), $"bytes")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
      .withWatermark("timestamp","30 seconds")
      .groupBy(window($"timestamp", "1 minutes"), col(aggColumn)) //aqui deberian ser 5 min, pero para que vaya mas rapido
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit(s"${aggColumn}_total_bytes"))
      .select( $"window.start".as("timestamp"), col(aggColumn).as("id"), $"value", $"type")

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
      .select(
        $"id", $"antenna_id", $"bytes", $"app",
        year($"timestamp".cast(TimestampType)).as("year"),
        month($"timestamp".cast(TimestampType)).as("month"),
        dayofmonth($"timestamp".cast(TimestampType)).as("day"),
        hour($"timestamp".cast(TimestampType)).as("hour")
      )
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
