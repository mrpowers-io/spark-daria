package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import SparkSessionExt._
import FunctionsAsColumnExt._
import utest._

object FunctionsAsColumnExtTest
    extends TestSuite
    with DataFrameComparer
    with ColumnComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'lower - {

      "lower cases a column" - {

        val df = spark.createDF(
          List(
            ("ThIS is COOL", "this is cool"),
            ("HAPPy", "happy"),
            (null, null)
          ), List(
            ("some_string", StringType, true),
            ("expected", StringType, true)
          )
        ).withColumn(
            "some_string_lower",
            col("some_string").lower()
          )

        assertColumnEquality(df, "expected", "some_string_lower")

      }

    }

  }

}
