package com.comcast.vops.perf.splunk

import java.util.Calendar
import org.scalatest._

class DateUtilitiesSpec extends FlatSpec with Matchers {
    "formatDate" should "match YYYY-MM-DD" in {
        val t = 0L
        DateUtilities.formatDate(t).length should be (10)
        DateUtilities.formatDate(t).matches("\\d\\d\\d\\d-\\d\\d-\\d\\d") should be (true)
    }

    "extractHour" should "return a number between 0 and 23" in {
        val offsets = List(100, 200, 500, 1000, 5000, 10000, 12500)
        val now = Calendar.getInstance().getTimeInMillis()
        val hours = offsets.map(now - _).map(DateUtilities.extractHour(_))
        all (hours) should be >= 0
        all (hours) should be < 24
    }

    "getMinute" should "return 0, 15, 30, 45" in {
        val offsets = List(101, 213, 573, 1123, 5830, 11203, 12523)
        val now = Calendar.getInstance().getTimeInMillis()
        val minutes = offsets.map(now - _).map(DateUtilities.getMinute(_, 15))
        val mod15s = minutes.map(_%15)
        all (minutes) should be >= 0
        all (minutes) should be < 46
        all (mod15s) should be (0)
    }

    "todaysDate" should "match today's date" in {
        val t = Calendar.getInstance().getTimeInMillis()
        DateUtilities.todaysDate() should be (DateUtilities.formatDate(t))
    }

    "yesterdaysDate" should "match yesterday's date" in {
        val t = Calendar.getInstance().getTimeInMillis() - 24*60*60*1000
        DateUtilities.yesterdaysDate() should be (DateUtilities.formatDate(t))
    }
}