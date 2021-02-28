package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def computeTotalBytes(dataFrame: DataFrame, aggColumn: String): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args

    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val telemetryDF = parserJsonData(kafkaDF)

    val totalBytesAntennaDF = computeTotalBytes(telemetryDF.withColumnRenamed("antenna_id", "antenna"), aggColumn = "antenna")
    val totalBytesUserDF = computeTotalBytes(telemetryDF.withColumnRenamed("id", "user"), aggColumn = "user")
    val totalBytesAppDF = computeTotalBytes(telemetryDF, aggColumn = "app")


    val storageFuture = writeToStorage(telemetryDF, storagePath)

    val toPostgres1 = writeToJdbc(totalBytesAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val toPostgres2 = writeToJdbc(totalBytesUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val toPostgres3 = writeToJdbc(totalBytesAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(toPostgres1, toPostgres2,toPostgres3, storageFuture)), Duration.Inf)


    spark.close()

  }

}
