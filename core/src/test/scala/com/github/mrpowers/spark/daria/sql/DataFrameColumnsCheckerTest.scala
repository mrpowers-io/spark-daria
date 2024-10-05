package com.github.mrpowers.spark.daria.sql

import utest._
import SparkSessionExt._
import org.apache.spark.sql.types.StringType

object DataFrameColumnsCheckerTest extends TestSuite with SparkSessionTestWrapper {

  val tests = Tests {

    'missingColumns - {

      "returns the columns missing from a DataFrame" - {

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

        val requiredColNames = Seq(
          "team",
          "sport",
          "country",
          "city"
        )

        val c = new DataFrameColumnsChecker(
          sourceDF,
          requiredColNames
        )

        assert(
          c.missingColumns == List(
            "country",
            "city"
          )
        )

      }

      "returns the empty list if columns aren't missing" - {

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

        val requiredColNames = Seq("team")

        val c = new DataFrameColumnsChecker(
          sourceDF,
          requiredColNames
        )

        assert(c.missingColumns == List())

      }

    }

    'missingColumnsMessage - {

      "provides a descriptive message when columns are missing" - {

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

        val requiredColNames = Seq(
          "team",
          "sport",
          "country",
          "city"
        )

        val v = new DataFrameColumnsChecker(
          sourceDF,
          requiredColNames
        )

        val expected =
          "The [country, city] columns are not included in the DataFrame with the following columns [team, sport]"

        assert(v.missingColumnsMessage() == expected)

      }

    }

    'validatePresenceOfColumns - {

      "throws an exception if columns are missing from a DataFrame" - {

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

        val requiredColNames = Seq(
          "team",
          "sport",
          "country",
          "city"
        )

        val v = new DataFrameColumnsChecker(
          sourceDF,
          requiredColNames
        )

        val e = intercept[MissingDataFrameColumnsException] {
          v.validatePresenceOfColumns()
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

        val requiredColNames = Seq("team")

        val v = new DataFrameColumnsChecker(
          sourceDF,
          requiredColNames
        )
        v.validatePresenceOfColumns()

      }

    }

  }

}
