package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import SparkSessionExt._
import org.apache.spark.sql.types.StringType

class DataFrameColumnsAbsenceSpec
    extends FunSpec
    with SparkSessionTestWrapper {

  describe("extraColNames") {

    it("returns the prohibitied colNames in a DataFrame") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football"),
          ("nacional", "soccer")
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true)
        )
      )

      val prohibitedColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      assert(c.extraColNames === List("team", "sport"))

    }

    it("returns the empty list if there aren't any extra columns") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football"),
          ("nacional", "soccer")
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true)
        )
      )

      val prohibitedColNames = Seq("ttt", "zzz")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      assert(c.extraColNames === List())

    }

  }

  describe("#extraColumnsMessage") {

    it("provides a descriptive message when extra columns are provided") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football"),
          ("nacional", "soccer")
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true)
        )
      )

      val prohibitedColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      val expected = "The [team, sport] columns are not allowed to be included in the DataFrame with the following columns [team, sport]"

      assert(c.extraColumnsMessage() === expected)

    }

  }

  describe("#validateAbsenceOfColumns") {

    it("throws an exception if prohibited columns are included in the DataFrame") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football"),
          ("nacional", "soccer")
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true)
        )
      )

      val prohibitedColNames = Seq("team", "sport", "country", "city")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      intercept[ProhibitedDataFrameColumnsException] {
        c.validateAbsenceOfColumns()
      }

    }

    it("does nothing if columns aren't missing") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football"),
          ("nacional", "soccer")
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true)
        )
      )

      val prohibitedColNames = Seq("ttt", "zzz")

      val c = new DataFrameColumnsAbsence(sourceDF, prohibitedColNames)

      c.validateAbsenceOfColumns()

    }

  }

}
