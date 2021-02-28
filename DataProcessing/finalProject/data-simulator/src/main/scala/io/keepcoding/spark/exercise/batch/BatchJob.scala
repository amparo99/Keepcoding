package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent._
import ExecutionContext.Implicits.global


trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def computeTotalBytes(storageDF: DataFrame, aggColumn: String, filterDate: OffsetDateTime): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def computeQuotaExceeded(userBytes: DataFrame, metadataDF: DataFrame): DataFrame

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val storageDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)

    val userBytes = computeTotalBytes(storageDF.withColumnRenamed("id", "user"), aggColumn = "user", filterDate = OffsetDateTime.parse(filterDate))
    val antennaBytes = computeTotalBytes(storageDF.withColumnRenamed("antenna_id", "antenna"), aggColumn = "antenna", filterDate = OffsetDateTime.parse(filterDate))
    val appBytes = computeTotalBytes(storageDF, aggColumn = "app", filterDate = OffsetDateTime.parse(filterDate))

    val userQuotaExceeded = computeQuotaExceeded(userBytes, metadataDF)

    println("metadata:")
    metadataDF.show()
    println("userBytes:")
    userBytes.show()
    println("users that exceeded the quota:")
    userQuotaExceeded.show()

    val toPostgres1 = writeToJdbc(userBytes, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    val toPostgres2 = writeToJdbc(antennaBytes, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    val toPostgres3 = writeToJdbc(appBytes, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    val toPostgres4 = writeToJdbc(userQuotaExceeded, jdbcUri, "user_quota_limit", jdbcUser, jdbcPassword)

    Await.result(
      Future.sequence(Seq(
        toPostgres1,
        toPostgres2,
        toPostgres3,
        toPostgres4
      )), Duration.Inf
    )
  }

}
