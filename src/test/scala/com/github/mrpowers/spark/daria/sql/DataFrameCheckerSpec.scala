package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

class DataFrameCheckerSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("missingColumns") {

    it("returns the columns missing from a DataFrame") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameChecker(sourceDf, requiredColNames)

      assert(c.missingColumns === List("country", "city"))

    }

    it("returns the empty list if columns aren't missing") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team")

      val c = new DataFrameChecker(sourceDf, requiredColNames)

      assert(c.missingColumns === List())

    }

  }

  describe("#missingColumnsMessage") {

    it("provides a descriptive message when columns are missing") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      val v = new DataFrameChecker(sourceDf, requiredColNames)

      val expected = "The [country, city] columns are not included in the DataFrame with the following columns [team, sport]"

      assert(v.missingColumnsMessage() === expected)

    }

  }

  describe("#validatePresenceOfColumns") {

    it("throws an exception if columns are missing from a DataFrame") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      val v = new DataFrameChecker(sourceDf, requiredColNames)

      intercept[MissingDataFrameColumnsException] {
        v.validatePresenceOfColumns()
      }


    }

    it("does nothing if columns aren't missing") {

      val sourceDf = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team")

      val v = new DataFrameChecker(sourceDf, requiredColNames)
      v.validatePresenceOfColumns()

    }

  }

}
