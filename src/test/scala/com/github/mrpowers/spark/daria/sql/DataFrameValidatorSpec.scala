package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

class DataFrameValidatorSpec extends FunSpec with DataFrameSuiteBase with DataFrameValidator {

  import spark.implicits._

  describe("#validatePresenceOfColumns") {

    it("throws an exception if columns are missing from a DataFrame") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      intercept[MissingDataFrameColumnsException] {
        validatePresenceOfColumns(sourceDf, requiredColNames)
      }


    }

    it("does nothing if columns aren't missing") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team")

      validatePresenceOfColumns(sourceDf, requiredColNames)

    }

  }

}
