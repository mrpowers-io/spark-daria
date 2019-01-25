package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.daria.sql.FunctionsAsColumnExt._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utest._

object FunctionsAsColumnExtTest
    extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {

    'initcap - {

      val df = spark
        .createDF(
          List(
            ("ThIS is COOL", "This Is Cool"),
            ("HAPPy", "Happy"),
            (null, null)
          ),
          List(
            ("some_string", StringType, true),
            ("expected", StringType, true)
          )
        )
        .withColumn(
          "res",
          col("some_string")|initcap
        )

      assertColumnEquality(
        df,
        "expected",
        "res"
      )

    }

    'length - {

      val df = spark
        .createDF(
          List(
            ("this", 4),
            ("hi", 2),
            (null, null)
          ),
          List(
            ("some_string", StringType, true),
            ("expected", IntegerType, true)
          )
        )
        .withColumn(
          "res",
          col("some_string")|length
        )

      assertColumnEquality(
        df,
        "expected",
        "res"
      )

    }

    'regexp_replace - {

      val df = spark
          .createDF(
            List(
              ("this", "THLS"),
              ("hi", "HL"),
              (null, null)
            ),
            List(
              ("some_string", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "res",
            col("some_string")| (regexp_replace(_, "i","l")) |upper
          )

      assertColumnEquality(
        df,
        "expected",
        "res"
      )

    }

    'lower - {

      val df = spark
        .createDF(
          List(
            ("ThIS is COOL", "this is cool"),
            ("HAPPy", "happy"),
            (null, null)
          ),
          List(
            ("some_string", StringType, true),
            ("expected", StringType, true)
          )
        )
        .withColumn(
          "some_string_lower",
          col("some_string")|lower
        )

      assertColumnEquality(
        df,
        "expected",
        "some_string_lower"
      )

    }

  }

}
