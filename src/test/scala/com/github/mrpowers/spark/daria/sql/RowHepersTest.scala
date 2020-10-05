package com.github.mrpowers.spark.daria.sql

import java.sql.Date
import java.util.Calendar

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Row}
import utest._

object RowHepersTest extends TestSuite with SparkSessionTestWrapper {

  import spark.implicits._

  val tests = Tests {

    "safeGetDate" - {

      "gets safely a date from a dataframe when dealing with a java.sql.Date" - {
        val data = List(
          HasReadingDate(Date.valueOf("2019-02-04"), 1),
          HasReadingDate(Date.valueOf("2019-02-05"), 1),
          HasReadingDate(Date.valueOf("2019-02-06"), 1),
          HasReadingDate(Date.valueOf("2019-02-07"), 1),
          HasReadingDate(Date.valueOf("2019-02-08"), 1),
          HasReadingDate(Date.valueOf("2019-02-09"), 1),
          HasReadingDate(Date.valueOf("2019-02-10"), 1)
        )
        val frame = data.toDF()

        val value = frame.transform(AddWeekendStage).as[HasReadingDateWithWeekend].collect().toSet

        assert(
          Set(
            HasReadingDateWithWeekend(Date.valueOf("2019-02-04"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-05"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-06"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-07"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-08"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-09"), 1, 1),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-10"), 1, 1)
          ) == value
        )
      }

      "gets safely a date from a dataframe when dealing with a String" - {
        val data = List(
          HasReadingDateString("2019-02-04", 1),
          HasReadingDateString("2019-02-05", 1),
          HasReadingDateString("2019-02-06", 1),
          HasReadingDateString("2019-02-07", 1),
          HasReadingDateString("2019-02-08", 1),
          HasReadingDateString("2019-02-09", 1),
          HasReadingDateString("2019-02-10", 1)
        )
        val frame = data.toDF()

        val value = frame.transform(AddWeekendStage).as[HasReadingDateWithWeekend].collect().toSet

        assert(
          Set(
            HasReadingDateWithWeekend(Date.valueOf("2019-02-04"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-05"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-06"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-07"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-08"), 1, 0),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-09"), 1, 1),
            HasReadingDateWithWeekend(Date.valueOf("2019-02-10"), 1, 1)
          ) == value
        )
      }

    }
  }

}

/**
 * This function adds an ""is_weekend" column
 * to the dataframe.
 * It is able to cope with different dats formats, either true dates or stringified dates (sql formats)
 */
case object AddWeekendStage extends (DataFrame => DataFrame) {

  import com.github.mrpowers.spark.daria.sql.RowHelpers._

  override def apply(df: DataFrame): DataFrame = {
    df.withColumn("is_weekend", isWeekendUDF(struct(col("reference_date"))))
  }

  def isWeekend(date: Date): Int = {
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) 1 else 0
  }

  /**
   * using the safeGetDate
   *
   * @param row
   * @return
   */
  def isWeekendRow(row: Row): Int =
    isWeekend(row.safeGetDate(0))

  val isWeekendUDF: UserDefinedFunction = udf(isWeekendRow _)

}

case class HasReadingDate(reference_date: Date, count: Int)
case class HasReadingDateString(reference_date: String, count: Int)
case class HasReadingDateWithWeekend(reference_date: Date, count: Int, is_weekend: Int)
