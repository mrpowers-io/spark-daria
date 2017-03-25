package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class DataFrameValidatorSpec extends FunSpec with DataFrameSuiteBase with DataFrameValidator {

  import spark.implicits._

  describe("#validatePresenceOfColumns") {

    it("throws an exception if columns are missing from a DataFrame") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team", "sport", "country", "city")

      intercept[MissingDataFrameColumnsException] {
        validatePresenceOfColumns(sourceDF, requiredColNames)
      }

    }

    it("does nothing if columns aren't missing") {

      val sourceDF = Seq(
        ("jets", "football"),
        ("nacional", "soccer")
      ).toDF("team", "sport")

      val requiredColNames = Seq("team")

      validatePresenceOfColumns(sourceDF, requiredColNames)

    }

  }

  describe("#validateSchema") {

    it("throws an exceptions if a required StructField is missing") {

      val sourceData = List(
        Row(1, 1),
        Row(-8, 8),
        Row(-5, 5),
        Row(null, null)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val requiredSchema = StructType(
        List(
          StructField("num1", IntegerType, true),
          StructField("num2", IntegerType, true),
          StructField("name", StringType, true)
        )
      )

      intercept[InvalidDataFrameSchemaException] {
        validateSchema(sourceDF, requiredSchema)
      }

    }

    it("does nothing if there aren't any StructFields missing") {

      val sourceData = List(
        Row(1, 1),
        Row(-8, 8),
        Row(-5, 5),
        Row(null, null)
      )

      val sourceSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val sourceDF = spark.createDataFrame(
        spark.sparkContext.parallelize(sourceData),
        StructType(sourceSchema)
      )

      val requiredSchema = StructType(
        List(
          StructField("num1", IntegerType, true)
        )
      )

      validateSchema(sourceDF, requiredSchema)

    }

  }

}
