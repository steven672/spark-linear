package com.comcast.vops.perf.splunk

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

object DateUtilities {
    val formatDate = (ts: Long) => Instant.ofEpochMilli(ts).atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_LOCAL_DATE)
    val formatDateKey = (ts: Long) => Instant.ofEpochMilli(ts).atZone(ZoneId.of("Z")).format(DateTimeFormatter.BASIC_ISO_DATE).take(8)
    val extractHour = (ts: Long) => Instant.ofEpochMilli(ts).atZone(ZoneId.of("Z")).format(DateTimeFormatter.ofPattern("H")).toInt
    val extractMinute = (ts: Long) => Instant.ofEpochMilli(ts).atZone(ZoneId.of("Z")).format(DateTimeFormatter.ofPattern("m")).toInt
    val floor = (x: Int, inc: Int) => x-(x%inc)
    val getMinute = (ts: Long, inc: Int) => floor(extractMinute(ts), inc)
    val todaysDate = () => formatDate(Instant.now().toEpochMilli())
    val yesterdaysDate =() => formatDate(Instant.now().minusSeconds(24*60*60).toEpochMilli())
    def parseTimestamp(s: String): Long = {
        val i = Instant.parse(s.replaceFirst(" ", "T").replace("0 UTC", "Z"))
        return i.toEpochMilli
    }
    val getMinuteString = (s: String, inc: Int) => floor(extractMinute(parseTimestamp(s)), inc)
    val getHourString = (s: String) => extractHour(parseTimestamp(s))
    val getDateKeyString = (s: String) => formatDateKey(parseTimestamp(s)).toInt
}