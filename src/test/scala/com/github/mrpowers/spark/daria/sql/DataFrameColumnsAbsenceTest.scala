package com.github.mrpowers.spark.daria.sql

import SparkSessionExt._
import org.apache.spark.sql.types.StringType

import utest._

object DataFrameColumnsAbsenceTest extends TestSuite with SparkSessionTestWrapper {

  val tests = Tests {

    'extraColNames - {

      "returns the prohibitied colNames in a DataFrame" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "football"),
              ("nacional", "soccer")
            ),
            List(
              ("team", StringType, true),
              ("sport", StringType, true)
            )
          )

        val prohibitedColNames = Seq(
          "team",
          "sport",
          "country",
          "city"
        )

        val c = new DataFrameColumnsAbsence(
          sourceDF,
          prohibitedColNames
        )

        assert(
          c.extraColNames == List(
            "team",
            "sport"
          )
        )

      }

      "returns the empty list if there aren't any extra columns" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "football"),
              ("nacional", "soccer")
            ),
            List(
              ("team", StringType, true),
              ("sport", StringType, true)
            )
          )

        val prohibitedColNames = Seq(
          "ttt",
          "zzz"
        )

        val c = new DataFrameColumnsAbsence(
          sourceDF,
          prohibitedColNames
        )

        assert(c.extraColNames == List())

      }

    }

    'extraColumnsMessage - {

      "provides a descriptive message when extra columns are provided" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "football"),
              ("nacional", "soccer")
            ),
            List(
              ("team", StringType, true),
              ("sport", StringType, true)
            )
          )

        val prohibitedColNames = Seq(
          "team",
          "sport",
          "country",
          "city"
        )

        val c = new DataFrameColumnsAbsence(
          sourceDF,
          prohibitedColNames
        )

        val expected =
          "The [team, sport] columns are not allowed to be included in the DataFrame with the following columns [team, sport]"

        assert(c.extraColumnsMessage() == expected)

      }

    }

    'validateAbsenceOfColumns - {

      "throws an exception if prohibited columns are included in the DataFrame" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "football"),
              ("nacional", "soccer")
            ),
            List(
              ("team", StringType, true),
              ("sport", StringType, true)
            )
          )

        val prohibitedColNames = Seq(
          "team",
          "sport",
          "country",
          "city"
        )

        val c = new DataFrameColumnsAbsence(
          sourceDF,
          prohibitedColNames
        )

        val e = intercept[ProhibitedDataFrameColumnsException] {
          c.validateAbsenceOfColumns()
        }

      }

      "does nothing if columns aren't missing" - {

        val sourceDF =
          spark.createDF(
            List(
              ("jets", "football"),
              ("nacional", "soccer")
            ),
            List(
              ("team", StringType, true),
              ("sport", StringType, true)
            )
          )

        val prohibitedColNames = Seq(
          "ttt",
          "zzz"
        )

        val c = new DataFrameColumnsAbsence(
          sourceDF,
          prohibitedColNames
        )

        c.validateAbsenceOfColumns()

      }

    }

  }

}
