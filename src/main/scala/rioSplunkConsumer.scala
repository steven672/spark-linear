package com.comcast.vops.perf.splunk

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import scala.io._

object RioSplunkConsumer {

  def main(args: Array[String]) {
    if (args.length < 1) {
     System.err.println("Usage: RioSplunkConsumer -- must supply Rio component as argument")
     System.exit(1)
    }
    val component = args(0)
    val spark = getSparkSession(args)
    val dataframe = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("hdfs://spark-master.vops.comcast.net/rio/" + component + ".csv")
    dataframe.createOrReplaceTempView("dataframe")
    val dfWithTime = spark.sql("""SELECT
      FLOOR(CAST(SUBSTR(from_utc_timestamp(CONCAT(CONCAT(SUBSTR(_time,0,10),' '), SUBSTR(_time,12,5)),'EST'),15,2)as int)/15)*15 as minute,
      SUBSTR(from_utc_timestamp(CONCAT(CONCAT(SUBSTR(_time,0,10),' '),SUBSTR(_time,12,5)),'EST'),12,2) as hour,
      SUBSTR(from_utc_timestamp(CONCAT(CONCAT(SUBSTR(_time,0,10),' '),SUBSTR(_time,12,5)),'EST'),0,10) as date,
      cRegion,
      host,
      streamID,
      Failures as ErrorCount,
      Successes as SuccessCount,
      successRate as SuccessRate,
      _time as timestamp
    from dataframe""")
    val aggregate = dfWithTime.groupBy("date", "hour", "minute", "cRegion", "host", "streamID").agg(sum("ErrorCount"), sum("SuccessCount"), (sum("SuccessRate")))
    val inserts = mapToInserts(aggregate, spark, component)
    inserts.rdd.foreachPartition { insertPartitions => RioInsert.batchInsert(insertPartitions, component) }
    // val reorderedColumns = Array("date", "hour", "minute","cRegion", "host", "streamID", "ErrorCount", "SuccessCount", "SuccessRate","timestamp")
    // val dfReordered = dfWithTime.select(reorderedColumns.head, reorderedColumns.tail: _*)
    // val rawInserts = mapToInsertsRaw(dfReordered, spark)
    // rawInserts.rdd.saveToCassandra("rio", component+"_raw")
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val component = args(0)
    // Basic configuration options
    return SparkSession
      .builder()
      .appName("rio-consumer-"+component)
      .master(sys.env.getOrElse("SPARK_MASTER", "spark://spark-master.vops.comcast.net:7077"))
      .config("spark.memory.storageFraction","0")
      .config("spark.local.dir", "/tmp/spark")
      .config("spark.cassandra.connection.host", getCassandraHosts())
      .getOrCreate()
  }

  def getCassandraHosts(): String = {
    return "96.118.158.29,96.118.149.224,96.118.149.218,96.118.128.61,96.118.128.46,96.118.128.48,96.118.128.64,96.118.128.56"
  }

  def mapToInserts(df: Dataset[Row], spark: SparkSession, component: String): Dataset[RioInsert] = {
    import spark.implicits._
    return df.map(x => RioInsert.fromRow(x, component))
  }

  def mapToInsertsRaw(df: Dataset[Row], spark: SparkSession): Dataset[RioInsert] = {
    import spark.implicits._
    return df.map(RioInsert.fromRawRow)
  }
}