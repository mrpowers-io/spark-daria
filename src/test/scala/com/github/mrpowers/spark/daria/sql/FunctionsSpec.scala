package com.github.mrpowers.spark.daria.sql

import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import SparkSessionExt._

class FunctionsSpec
  extends FunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {

  describe("#yeardiff") {

    it("calculates the years between two dates") {

      val testDF = spark.createDF(
        List(
          (Timestamp.valueOf("2016-09-10 00:00:00"), Timestamp.valueOf("2001-08-10 00:00:00")),
          (Timestamp.valueOf("2016-04-18 00:00:00"), Timestamp.valueOf("2010-05-18 00:00:00")),
          (Timestamp.valueOf("2016-01-10 00:00:00"), Timestamp.valueOf("2013-08-10 00:00:00")),
          (null, null)
        ), List(
          ("first_datetime", TimestampType, true),
          ("second_datetime", TimestampType, true)
        )
      )

      val actualDF = testDF
        .withColumn(
          "num_years",
          functions.yeardiff(col("first_datetime"), col("second_datetime"))
        )

      val expectedDF = spark.createDF(
        List(
          (15.095890410958905),
          (5.923287671232877),
          (2.419178082191781),
          (null)
        ), List(
          ("num_years", DoubleType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF.select("num_years"), expectedDF)

    }

  }

  describe("#between") {

    it("calculates the values between two criteria") {

      val sourceDF = spark.createDF(
        List(
          (1),
          (5),
          (8),
          (15),
          (39),
          (55)
        ), List(
          ("number", IntegerType, true)
        )
      )

      val actualDF = sourceDF.where(functions.between(col("number"), 8, 39))

      val expectedDF = spark.createDF(
        List(
          (8),
          (15),
          (39)
        ), List(
          ("number", IntegerType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("works for dates") {

      val sourceDF = spark.createDF(
        List(
          (Date.valueOf("1980-09-10")),
          (Date.valueOf("1990-04-18")),
          (Date.valueOf("2000-04-18")),
          (Date.valueOf("2010-04-18")),
          (Date.valueOf("2016-01-10"))
        ), List(
          ("some_date", DateType, true)
        )
      )

      val actualDF = sourceDF.where(
        functions.between(
          col("some_date"),
          "1999-04-18",
          "2011-04-18"
        )
      )

      val expectedDF = spark.createDF(
        List(
          (Date.valueOf("2000-04-18")),
          (Date.valueOf("2010-04-18"))
        ), List(
          ("some_date", DateType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#capitalizeFully") {

    it("uses the supplied delimeter to identify word breaks with org.apache.commons WordUtils.capitalizeFully") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce,willis"),
          ("Trump,donald"),
          ("clinton,Hillary"),
          ("Brack obama"),
          ("george w. bush"),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.select(
        functions.capitalizeFully(List(','))(col("some_string")).as("some_string_udf")
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce,Willis"),
          ("Trump,Donald"),
          ("Clinton,Hillary"),
          ("Brack obama"),
          ("George w. bush"),
          (null)
        ), List(
          ("some_string_udf", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#exists") {

    it("returns true if the array includes a value that makes the function return true") {

      val sourceDF = spark.createDF(
        List(
          (Array(1, 4, 9)),
          (Array(1, 3, 5))
        ), List(
          ("nums", ArrayType(IntegerType, true), true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "nums_has_even",
        functions.exists[Int]((x: Int) => x % 2 == 0).apply(col("nums"))
      )

      val expectedDF = spark.createDF(
        List(
          (Array(1, 4, 9), true),
          (Array(1, 3, 5), false)
        ), List(
          ("nums", ArrayType(IntegerType, true), true),
          ("nums_has_even", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#forall") {

    it("works like the Scala forall method") {

      val sourceDF = spark.createDF(
        List(
          (Array("snake", "rat")),
          (Array("cat", "crazy"))
        ), List(
          ("words", ArrayType(StringType, true), true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "all_words_begin_with_c",
        functions.forall[String]((x: String) => x.startsWith("c")).apply(col("words"))
      )

      val expectedDF = spark.createDF(
        List(
          (Array("snake", "rat"), false),
          (Array("cat", "crazy"), true)
        ), List(
          ("words", ArrayType(StringType, true), true),
          ("all_words_begin_with_c", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("boolAnd") {
    it("returns true if all specified input columns satisfy the And condition") {

      val sourceData = List(
        (true, false, true, false),
        (false, false, true, false),
        (true, true, true, true),
        (true, true, false, false),
        (true, true, true, false)
      )

      val sourceSchema = List(
        ("c1", BooleanType, true),
        ("c2", BooleanType, true),
        ("c3", BooleanType, true),
        ("c4", BooleanType, true)
      )

      val sourceDF = spark.createDF(sourceData, sourceSchema)

      val actualDF = sourceDF.withColumn("valid_flag", functions.boolAnd(true, col("c1"), col("c2")) &&
        functions.boolAnd(false, col("c3"), col("c4")))

      val expectedData = List(
        (true, false, true, false, false),
        (false, false, true, false, false),
        (true, true, true, true, false),
        (true, true, false, false, true),
        (true, true, true, false, false)
      )

      val expectedSchema = sourceSchema ::: List(("valid_flag", BooleanType, true))

      val expectedDF = spark.createDF(expectedData, expectedSchema)

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
