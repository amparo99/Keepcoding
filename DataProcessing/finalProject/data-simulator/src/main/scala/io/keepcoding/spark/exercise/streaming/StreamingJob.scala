package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(telemetryDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeTotalBytesAntenna(dataFrame: DataFrame): DataFrame

  def computeTotalBytesUser(dataFrame: DataFrame): DataFrame

  def computeTotalBytesApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args

    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val telemetryDF = parserJsonData(kafkaDF)
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val enrichDF = enrichAntennaWithMetadata(telemetryDF, metadataDF)

    val totalbytesAntennaDF = computeTotalBytesAntenna(enrichDF)
    val totalbytesUserDF = computeTotalBytesUser(enrichDF)
    val totalbytesAppDF = computeTotalBytesApp(enrichDF)


    //val storageFuture1 = writeToStorage(totalbytesAntennaDF, storagePath)
    //val storageFuture2 = writeToStorage(totalbytesUserDF, storagePath)
    val storageFuture3 = writeToStorage(totalbytesAppDF, storagePath)

    val toPostgres1 = writeToJdbc(totalbytesAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val toPostgres2 = writeToJdbc(totalbytesUserDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val toPostgres3 = writeToJdbc(totalbytesAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(toPostgres1,toPostgres2, toPostgres3, storageFuture3)), Duration.Inf)


    spark.close()

  }

}
