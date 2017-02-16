package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.scalatest.FunSpec

class FunctionsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#yeardiff") {

    it("calculates the years between two dates") {

      val testDf = Seq(
        ("2016-09-10", "2001-08-10"),
        ("2016-04-18", "2010-05-18"),
        ("2016-01-10", "2013-08-10")
      )
      .toDF("first_datetime", "second_datetime")
      .withColumn("first_datetime", $"first_datetime".cast("timestamp"))
      .withColumn("second_datetime", $"second_datetime".cast("timestamp"))

      val actualDf = testDf
        .withColumn("num_years", functions.yeardiff(col("first_datetime"), col("second_datetime")))

      val expectedSchema = List(
        StructField("num_years", DoubleType, true)
      )

      val expectedData = Seq(
        Row(15.1),
        Row(5.9),
        Row(2.4)
      )

      val expectedDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameApproximateEquals(actualDf.select("num_years"), expectedDf, 0.1)

    }

  }

  describe("#between") {

    it("calculates the values between two criteria") {

      val sourceDf = Seq(
        (1),
        (5),
        (8),
        (15),
        (39),
        (55)
      ).toDF("number")

      val actualDf = sourceDf.where(functions.between(col("number"), 8, 39))

      val expectedDf = Seq(
        (8),
        (15),
        (39)
      ).toDF("number")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("works for dates") {

      val sourceDf = Seq(
        ("1980-09-10"),
        ("1990-04-18"),
        ("2000-04-18"),
        ("2010-04-18"),
        ("2016-01-10")
      ).toDF("some_date")
        .withColumn("some_date", $"some_date".cast("timestamp"))

      val actualDf = sourceDf.where(functions.between(col("some_date"), "1999-04-18", "2011-04-18"))

      val expectedDf = Seq(
        ("2000-04-18"),
        ("2010-04-18")
      ).toDF("some_date")
        .withColumn("some_date", $"some_date".cast("timestamp"))

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
