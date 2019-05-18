package com.github.mrpowers.spark.daria.sql

import utest._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import SparkSessionExt._

object DataFrameValidatorTest extends TestSuite with SparkSessionTestWrapper with DataFrameValidator {

  val tests = Tests {

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

        val e = intercept[MissingDataFrameColumnsException] {
          validatePresenceOfColumns(
            sourceDF,
            requiredColNames
          )
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

        validatePresenceOfColumns(
          sourceDF,
          requiredColNames
        )

      }

    }

    'validateSchema - {

      "throws an exceptions if a required StructField is missing" - {

        val sourceDF = spark.createDF(
          List(
            Row(
              1,
              1
            ),
            Row(
              -8,
              8
            ),
            Row(
              -5,
              5
            ),
            Row(
              null,
              null
            )
          ),
          List(
            ("num1", IntegerType, true),
            ("num2", IntegerType, true)
          )
        )

        val requiredSchema = StructType(
          List(
            StructField(
              "num1",
              IntegerType,
              true
            ),
            StructField(
              "num2",
              IntegerType,
              true
            ),
            StructField(
              "name",
              StringType,
              true
            )
          )
        )

        val e = intercept[InvalidDataFrameSchemaException] {
          validateSchema(
            sourceDF,
            requiredSchema
          )
        }

      }

      "does nothing if there aren't any StructFields missing" - {

        val sourceDF = spark.createDF(
          List(
            Row(
              1,
              1
            ),
            Row(
              -8,
              8
            ),
            Row(
              -5,
              5
            ),
            Row(
              null,
              null
            )
          ),
          List(
            ("num1", IntegerType, true),
            ("num2", IntegerType, true)
          )
        )

        val requiredSchema =
          StructType(
            List(
              StructField(
                "num1",
                IntegerType,
                true
              )
            )
          )

        validateSchema(
          sourceDF,
          requiredSchema
        )

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

        val e = intercept[ProhibitedDataFrameColumnsException] {
          validateAbsenceOfColumns(
            sourceDF,
            prohibitedColNames
          )
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

        validateAbsenceOfColumns(
          sourceDF,
          prohibitedColNames
        )

      }

    }

  }

}
