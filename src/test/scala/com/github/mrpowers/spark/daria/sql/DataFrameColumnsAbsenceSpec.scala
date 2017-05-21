package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec

class DataFrameColumnsAbsenceSpec
    extends FunSpec
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe("extraColNames") {

    it("returns the prohibitied colNames in a DataFrame") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val prohibitedColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      assert(c.extraColNames === List("team", "sport"))

    }

    it("returns the empty list if there aren't any extra columns") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val prohibitedColNames = Seq("ttt", "zzz")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      assert(c.extraColNames === List())

    }

  }

  describe("#extraColumnsMessage") {

    it("provides a descriptive message when extra columns are provided") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val prohibitedColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      val expected = "The [team, sport] columns are not allowed to be included in the DataFrame with the following columns [team, sport]"

      assert(c.extraColumnsMessage() === expected)

    }

  }

  describe("#validateAbsenceOfColumns") {

    it("throws an exception if prohibited columns are included in the DataFrame") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val prohibitedColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      intercept[ProhibitedDataFrameColumnsException] {
        c.validateAbsenceOfColumns()
      }

    }

    it("does nothing if columns aren't missing") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val prohibitedColNames = Seq("ttt", "zzz")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      c.validateAbsenceOfColumns()

    }

  }

}
