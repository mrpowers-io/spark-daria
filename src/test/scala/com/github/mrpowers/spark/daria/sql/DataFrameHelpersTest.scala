package com.github.mrpowers.spark.daria.sql

import utest._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

object DataFrameHelpersTest
    extends TestSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  val tests = Tests {

    'twoColumnsToMap - {

      "converts two columns of a DataFrame to a map" - {

        val sourceDF = spark.createDF(
          List(
            ("boracay", 7),
            ("long island", 9)
          ), List(
            ("island", StringType, true),
            ("fun_level", IntegerType, true)
          )
        )

        val actual = DataFrameHelpers.twoColumnsToMap[String, Integer](
          sourceDF,
          "island",
          "fun_level"
        )

        val expected = Map(
          "boracay" -> 7,
          "long island" -> 9
        )

        assert(actual == expected)

      }

    }

    'columnToArray - {

      "converts a column to an array" - {

        val sourceDF = spark.createDF(
          List(
            1,
            2,
            3
          ), List(
            ("num", IntegerType, true)
          )
        )

        val actual = DataFrameHelpers.columnToArray[Int](sourceDF, "num")

        actual ==> Array(1, 2, 3)

      }

    }

    'toArrayOfMaps - {

      "converts a DataFrame into an array of maps" - {

        val sourceDF = spark.createDF(
          List(
            ("doctor", 4, "high"),
            ("dentist", 10, "high")
          ), List(
            ("profession", StringType, true),
            ("some_number", IntegerType, true),
            ("pay_grade", StringType, true)
          )
        )

        val actual = DataFrameHelpers.toArrayOfMaps(sourceDF)

        val expected = Array(
          Map("profession" -> "doctor", "some_number" -> 4, "pay_grade" -> "high"),
          Map("profession" -> "dentist", "some_number" -> 10, "pay_grade" -> "high")
        )

        actual ==> expected

      }

    }

    'withOrderedIndex - {

      "appends an ordered_index column to a DataFrame" - {

        val sourceDF = spark.createDF(
          List(
            ("Bart cool", "moto cool"),
            ("cool James", "droid fun"),
            (null, null)
          ), List(
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        val actualDF = DataFrameHelpers.withOrderedIndex(sourceDF, spark)

        val expectedDF = spark.createDF(
          List(
            (0L, "Bart cool", "moto cool"),
            (1L, "cool James", "droid fun"),
            (2L, null, null)
          ), List(
            ("ordered_index", LongType, true),
            ("person", StringType, true),
            ("phone", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

    'smush - {

      val df1 = spark.createDF(
        List(
          ("bob", 10),
          ("frank", 15),
          (null, null)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val df2 = spark.createDF(
        List(
          ("bob", 12),
          ("frank", 15),
          (null, null)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val actualDF = DataFrameHelpers.smush(df1, df2, spark)

      val expectedDF = spark.createDF(
        List(
          ("bob", 10, "bob", 12),
          ("frank", 15, "frank", 15),
          (null, null, null, null)
        ), List(
          ("df1_name", StringType, true),
          ("df1_age", IntegerType, true),
          ("df2_name", StringType, true),
          ("df2_age", IntegerType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
