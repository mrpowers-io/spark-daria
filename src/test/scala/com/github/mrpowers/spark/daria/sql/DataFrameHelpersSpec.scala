package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.types.{IntegerType, StringType}
import SparkSessionExt._

class DataFrameHelpersSpec
    extends FunSpec
    with SparkSessionTestWrapper {

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

  describe(".withValueLookup") {

    it("looks up a value in a map") {

      val sourceDF = spark.createDF(
        List(
          ("doctor", 4),
          ("dentist", 10)
        ), List(
          ("profession", StringType, true),
          ("some_number", IntegerType, true)
        )
      )

      val maps = Array(
        Map(
          "profession1" -> "doctor",
          "some_number1" -> 4,
          "secret" -> "cool"
        ),
        Map(
          "profession1" -> "accountant",
          "some_number1" -> 8,
          "secret" -> "boring"
        )
      )

      val mapValue = "secret"

      val keyMappings = Map(
        "profession" -> "profession1",
        "some_number" -> "some_number1"
      )

      val actualDF = DataFrameHelpers.withValueLookup(sourceDF, maps, mapValue, keyMappings)

      actualDF.show()

    }

  }

}
