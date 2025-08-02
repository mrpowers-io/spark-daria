package org.apache.spark.sql

import utest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.BebeFunctions._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._
import java.time.ZonedDateTime
import java.util.TimeZone
import java.time.ZoneId
import java.time.Instant

object BebeFunctionsSpec extends TestSuite with SparkSessionTestWrapper with ColumnComparer with DataFrameComparer {

  import spark.implicits._

  // ADDITIONAL HELPER FUNCTIONS
  private val regex = s"([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})".r

  private def valueOf(date: String, zone: String): Instant = {
    date match {
      case regex(year, month, day, hour, minute, second) =>
        ZonedDateTime
          .of(
            year.toInt,
            month.toInt,
            day.toInt,
            hour.toInt,
            minute.toInt,
            second.toInt,
            0,
            ZoneId.of(zone)
          )
          .toInstant()
    }
  }

  override def tests =
    Tests {
      "beginningOfDay - returns the beginning of the day" - {
        val df = Seq(
          (Timestamp.valueOf("2020-01-15 08:01:32"), Timestamp.valueOf("2020-01-15 00:00:00")),
          (Timestamp.valueOf("2020-01-20 23:03:22"), Timestamp.valueOf("2020-01-20 00:00:00")),
          (null, null)
        ).toDF("some_time", "expected")
        val resDF = df.withColumn("actual", beginningOfDay(col("some_time")))
        assertColumnEquality(resDF, "actual", "expected")
      }

      "beginningOfDay - returns the beginning of the day in a specific timezone" - {
        val testTimezone         = "America/Bahia"
        val defaultTimezone      = TimeZone.getDefault()
        val sparkDefaultTimezone = spark.conf.get("spark.sql.session.timeZone")
        TimeZone.setDefault(TimeZone.getTimeZone(testTimezone))
        spark.conf.set("spark.sql.session.timeZone", testTimezone)
        val df = Seq(
          (
            valueOf("2020-01-15 08:01:32", testTimezone),
            valueOf("2020-01-14 21:00:00", testTimezone)
          ),
          (
            valueOf("2020-01-20 23:03:22", testTimezone),
            valueOf("2020-01-20 21:00:00", testTimezone)
          ),
          (null, null)
        ).toDF("some_time", "expected")
          .withColumn("actual", beginningOfDay(col("some_time"), "UTC"))
        assertColumnEquality(df, "actual", "expected")
        TimeZone.setDefault(defaultTimezone)
        spark.conf.set("spark.sql.session.timeZone", sparkDefaultTimezone)
      }

      // Add the rest of your test cases below in the same way.
      // For each `describe("foo") { it("bar") { ... } }` block,
      // convert to test("foo - bar") { ... } in uTest.

      // Example for 'beginningOfMonth':
      "beginningOfMonth - gets the beginning of the month of a date column" - {
        val df = Seq(
          (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-01")),
          (Date.valueOf("2020-01-20"), Date.valueOf("2020-01-01")),
          (null, null)
        ).toDF("some_date", "expected")
        val resDF = df.withColumn("actual", beginningOfMonth(col("some_date")))
        assertColumnEquality(resDF, "actual", "expected")
      }

      // Continue for all other describe/it blocks...
      // Each `it("desc")` inside a `describe("func")` becomes:
      // test("func - desc") { ... }
      // Keep the test logic between {}

      // For exception tests, use intercept[ExceptionType] { ... }
      // For example:
      "bebe_character_length - errors on wrong column type" - {
        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(
              Seq(
                Row(33),
                Row(44),
                Row(null)
              )
            ),
            StructType(List(StructField("some_int", IntegerType, nullable = true)))
          )
          .withColumn("actual", bebe_character_length(col("some_int")))
        intercept[org.apache.spark.sql.AnalysisException] {
          assertColumnEquality(df, "actual", "expected")
        }
      }

      // Repeat for all other tests...
    }
}
