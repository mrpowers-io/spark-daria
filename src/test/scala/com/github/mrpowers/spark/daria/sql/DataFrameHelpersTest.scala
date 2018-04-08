package com.github.mrpowers.spark.daria.sql

import utest._

import org.apache.spark.sql.types.{IntegerType, StringType}
import SparkSessionExt._

object DataFrameHelpersTest
    extends TestSuite
    with SparkSessionTestWrapper {

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

  }

}
