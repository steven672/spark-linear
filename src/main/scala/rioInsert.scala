package com.comcast.vops.perf.splunk

import org.apache.spark.sql.Row
import java.sql.Statement
import java.sql.BatchUpdateException


case class RioInsert(DATE_KEY: Integer, HOUR_KEY: Integer, MINUTE_KEY: Integer, REGION: String, HOST: String, STREAM_ID: Long, ERROR_COUNT: Integer, SUCCESS_COUNT: Integer, SUCCESS_RATE: Double, TIMESTAMP: String)

object RioInsert {
    def fromRow(row: Row, component: String): RioInsert = {
        try {
                return new RioInsert(
                    row.getString(0).replace("-","").toInt,
                    row.getInt(1),
                    row.getInt(2),
                    row.getString(3),
                    row.getString(4),
                    row.getLong(5),
                    row.getInt(6),
                    row.getInt(7),
                    extractDouble(row, 8)*100,
                    ""// row.getString(9)
                )
        } catch {
            case e: Exception => println(row); println(e); return blankInsert();
        }

    }

    def fromRawRow(row: Row): RioInsert = {
        try {
            return new RioInsert(
                row.getString(0).replace("-","").toInt,
                row.getInt(1),
                row.getInt(2),
                row.getString(3),
                row.getString(4),
                row.getLong(5),
                row.getInt(6),
                row.getInt(7),
                extractDouble(row, 8)*100,
                row.getString(9)
            )

        } catch {
            case e: Exception => println(row); println(e); return blankInsert();
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
    val blankInsert = () => RioInsert(0,0,0,"","",0.toLong,0,0,0.00,"")
    val insertQuery = (component: String) => "INSERT INTO " + getTable(component) + " (DATE_KEY, HOUR_KEY, MINUTE_KEY, REGION, HOST, STREAM_ID, ERROR_COUNT, SUCCESS_COUNT, SUCCESS_RATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val getTable = (component: String) => "rio_" + component.toLowerCase + "_availability_15m_a"
    def retryInsert(partition: Iterator[RioInsert], bue: BatchUpdateException, attempts: Int) = {
        println("\n RESET:")
        println(bue)
        Database.resetConnection()
    }
    def batchInsert(partition: Iterator[RioInsert], component: String): Unit = {
        batchInsert(partition, component, 0)
    }
    def batchInsert(partition: Iterator[RioInsert], component: String, attempts: Int): Unit = {
        var count = 0
        val connection = Database.provideConnection()
        val insertStatement = connection.prepareStatement(insertQuery(component))
        partition.foreach { i =>
            // Long, string, string, string, string, string, string, long
            insertStatement.setInt(1, i.DATE_KEY)
            insertStatement.setInt(2, i.HOUR_KEY)
            insertStatement.setInt(3, i.MINUTE_KEY)
            insertStatement.setString(4, i.REGION)
            insertStatement.setString(5, i.HOST)
            insertStatement.setLong(6, i.STREAM_ID)
            insertStatement.setInt(7, i.ERROR_COUNT)
            insertStatement.setInt(8, i.SUCCESS_COUNT)
            insertStatement.setDouble(9, i.SUCCESS_RATE)
            insertStatement.setString(10, i.TIMESTAMP)
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