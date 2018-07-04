package com.comcast.vops.perf.splunk

import org.apache.spark.sql.Row
import java.sql.Statement
import java.sql.BatchUpdateException


case class LinearInsert(DATE_KEY: Integer, HOUR_KEY: Integer, MINUTE_KEY: Integer, STREAM_NAME: String, STREAM_ID: Long, FACILITY: String, REGION: String, HOST: String, ERROR_COUNT: Long, TOTAL_COUNT: Long, ERROR_FREE: Double, TIMESTAMP: String, VERSION: String)

object LinearInsert {
    def fromRow(row: Row, component: String): LinearInsert = {
        try {
            if (component == "pillar") {
                return new LinearInsert(
                    row.getInt(0),
                    row.getInt(1),
                    row.getInt(2),
                    row.getString(3),
                    extractStreamId(row.getString(3)),
                    row.getString(4),
                    row.getString(5),
                    row.getString(6),
                    row.getLong(8),
                    row.getLong(9),
                    extractDouble(row, 10)*100,
                    "",
                    extractVersion(row, 7)
                )
            } else {
                return new LinearInsert(
                    row.getInt(0),
                    row.getInt(1),
                    row.getInt(2),
                    row.getString(3),
                    extractStreamId(row.getString(3)),
                    row.getString(4),
                    row.getString(5),
                    row.getString(6),
                    row.getLong(7),
                    row.getLong(8),
                    extractDouble(row, 9)*100,
                    "",
                    ""
                )
            }
        } catch {
            case e: Exception => println(row); println(e); return blankInsert();
        }

    }
    def fromRawRow(row: Row): LinearInsert = {
        try {
            return new LinearInsert(
                row.getInt(0),
                row.getInt(1),
                row.getInt(2),
                row.getString(3),
                extractStreamId(row.getString(3)),
                row.getString(4),
                row.getString(5),
                row.getString(6),
                row.getInt(7).toLong,
                row.getInt(8).toLong,
                extractDouble(row, 9),
                row.getString(10),
                extractVersion(row, 11)
            )
        } catch {
            case e: Exception => println(row); println(e); return blankInsert();
        }
    }
    def extractVersion (row: Row, position: Integer): String = {
        try {
            row.getString(position)
        } catch {
            case a: ArrayIndexOutOfBoundsException => ""
            case c: ClassCastException => ""
        }
    }
    def extractDouble (row: Row, position: Integer): Double = {
        try {
            row.getDouble(position)
        } catch {
            case a: ArrayIndexOutOfBoundsException => 0.0
            case c: ClassCastException => row.getInt(position).toDouble
        }
    }
    val blankInsert = () => LinearInsert(0,0,0,"",0.toLong,"","","",0.toLong,0.toLong,0.00,"","")
    val extractStreamId = (streamName: String) => streamName.takeRight(19).toLong
    val insertQuery = (component: String) => "INSERT INTO " + getTable(component) + " (DATE_KEY, HOUR_KEY, MINUTE_KEY, STREAM_NAME, STREAM_ID, FACILITY, REGION, ERROR_COUNT, TOTAL_COUNT, "+getAvailabilityColumn(component)+", HOST, VERSION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val getTable = (component: String) => "w_" + component.toLowerCase + "_raw_streams_15m_a"
    val getAvailabilityColumn = (component: String) => if(component == "pillar") "ERROR_FREE" else "AVAILABILITY"
    def retryInsert(partition: Iterator[LinearInsert], bue: BatchUpdateException, attempts: Int) = {
        println("\n RESET:")
        println(bue)
        Database.resetConnection()
    }
    def batchInsert(partition: Iterator[LinearInsert], component: String): Unit = {
        batchInsert(partition, component, 0)
    }
    def batchInsert(partition: Iterator[LinearInsert], component: String, attempts: Int): Unit = {
        var count = 0
        val connection = Database.provideConnection()
        val insertStatement = connection.prepareStatement(insertQuery(component))
        partition.foreach { i =>
            // Long, string, string, string, string, string, string, long
            insertStatement.setInt(1, i.DATE_KEY)
            insertStatement.setInt(2, i.HOUR_KEY)
            insertStatement.setInt(3, i.MINUTE_KEY)
            insertStatement.setString(4, i.STREAM_NAME)
            insertStatement.setLong(5, i.STREAM_ID)
            insertStatement.setString(6, i.FACILITY)
            insertStatement.setString(7, i.REGION)
            insertStatement.setLong(8, i.ERROR_COUNT)
            insertStatement.setLong(9, i.TOTAL_COUNT)
            insertStatement.setDouble(10, i.ERROR_FREE)
            insertStatement.setString(11, i.HOST)
            insertStatement.setString(12, i.VERSION)
            insertStatement.addBatch()
            count = count + 1
        }
        try {
            insertStatement.executeBatch()
            println("\n" + count.toString + " inserts")
        } catch {
            case bue: BatchUpdateException => retryInsert(partition, bue, attempts)
            case e: Exception => println(e)
        }
    }
}