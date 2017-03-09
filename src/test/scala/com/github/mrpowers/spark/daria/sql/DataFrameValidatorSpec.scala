package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

class DataFrameValidatorSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#missingColumns") {

    it("returns the columns missing from a DataFrame") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      val c = DataFrameValidator.missingColumns(sourceDf, requiredColNames)

      assert(c === List("country", "city"))

    }

    it("returns the empty list if columns aren't missing") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team")

      val c = DataFrameValidator.missingColumns(sourceDf, requiredColNames)

      assert(c === List())

    }

  }

  describe("#validatePresenceOfColumns") {

    it("throws an exception if columns are missing from a DataFrame") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      intercept[MissingDataFrameColumnsException] {
        DataFrameValidator.validatePresenceOfColumns(sourceDf, requiredColNames)
      }


    }

    it("does nothing if columns aren't missing") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team")

      DataFrameValidator.validatePresenceOfColumns(sourceDf, requiredColNames)

    }

  }

}
