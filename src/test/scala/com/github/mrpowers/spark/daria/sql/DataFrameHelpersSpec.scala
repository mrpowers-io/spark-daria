package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.types.{IntegerType, StringType}
import SparkSessionExt._

class DataFrameHelpersSpec
    extends FunSpec
    with SparkSessionTestWrapper {

  describe(".twoColumnsToMap") {

    it("converts two columns of a DataFrame to a map") {

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

      assert(actual === expected)

    }

  }

  describe(".toArrayOfMaps") {

    it("converts a DataFrame into an array of maps") {

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

      assert(actual === expected)

    }

  }

}
