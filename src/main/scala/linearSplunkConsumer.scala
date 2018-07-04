package com.comcast.vops.perf.splunk

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import scala.io._

object LinearSplunkConsumer {

  def main(args: Array[String]) {
    if (args.length < 1) {
     System.err.println("Usage: LinearSplunkConsumer -- must supply linear component as argument")
     System.exit(1)
    }
    val component = args(0)
    val spark = getSparkSession(args)
    val dataframe = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("hdfs://spark-master.vops.comcast.net/linear/" + component + "100streams1h.csv")
    val df = dataframe.withColumnRenamed("SuccessCount", "ErrorCount")
    val minute15 = DateUtilities.getMinuteString(_: String, 15)
    val minute15U = udf(minute15)
    val hour1 = DateUtilities.getHourString(_: String)
    val hourU = udf(hour1)
    val dateKey = DateUtilities.getDateKeyString(_: String)
    val dateKeyU = udf(dateKey)
    val dfWithTime = df.filter(length(col("title")) > 19).withColumn("minute", minute15U(col("_time"))).withColumn("hour", hourU(col("_time"))).withColumn("date", dateKeyU(col("_time")))
    val aggregate = if (component == "pillar")
      dfWithTime.groupBy("date", "hour", "minute", "title", "cFacility", "cRegion", "host", "version").agg(sum("ErrorCount"), sum("Total"), (sum("Total")-sum("ErrorCount"))/sum("Total"))
    else
      dfWithTime.groupBy("date", "hour", "minute", "title", "cFacility", "cRegion", "host").agg(sum("ErrorCount"), sum("Total"), (sum("Total")-sum("ErrorCount"))/sum("Total"))
    val inserts = mapToInserts(aggregate, spark, component)
    inserts.rdd.foreachPartition { insertPartitions => LinearInsert.batchInsert(insertPartitions, component) }
    // val reorderedColumns = if (component == "pillar")
      // Array("date", "hour", "minute", "title", "cFacility", "cRegion", "host", "ErrorCount", "Total", "availability%", "_time", "version")
    // else
      // Array("date", "hour", "minute", "title", "cFacility", "cRegion", "host", "ErrorCount", "Total", "availability%", "_time")
    // val dfReordered = dfWithTime.select(reorderedColumns.head, reorderedColumns.tail: _*)
    // val rawInserts = mapToInsertsRaw(dfReordered, spark)
    // rawInserts.rdd.saveToCassandra("linear", component+"_raw")
  }

  def getSparkSession(args: Array[String]): SparkSession = {
    val component = args(0)
    // Basic configuration options
    return SparkSession
      .builder()
      .appName("linear-consumer-"+component)
      .master(sys.env.getOrElse("SPARK_MASTER", "spark://spark-master.vops.comcast.net:7077"))
      .config("spark.memory.storageFraction","0")
      .config("spark.local.dir", "/tmp/spark")
      .config("spark.cassandra.connection.host", getCassandraHosts())
      .getOrCreate()
  }

  def getCassandraHosts(): String = {
    return "96.118.158.29,96.118.149.224,96.118.149.218,96.118.128.61,96.118.128.46,96.118.128.48,96.118.128.64,96.118.128.56"
  }

  def mapToInserts(df: Dataset[Row], spark: SparkSession, component: String): Dataset[LinearInsert] = {
    import spark.implicits._
    return df.map(x => LinearInsert.fromRow(x, component))
  }

  def mapToInsertsRaw(df: Dataset[Row], spark: SparkSession): Dataset[LinearInsert] = {
    import spark.implicits._
    return df.map(LinearInsert.fromRawRow)
  }
}