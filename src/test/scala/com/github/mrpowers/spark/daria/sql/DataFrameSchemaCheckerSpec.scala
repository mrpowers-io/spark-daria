package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class DataFrameSchemaCheckerSpec extends FunSpec with DataFrameSuiteBase {

  describe("missingStructFields") {

    it("returns the StructFields missing from a DataFrame") {

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

      val c = new DataFrameSchemaChecker(sourceDF, requiredSchema)

      assert(c.missingStructFields === List(StructField("name", StringType, true)))

    }

    it("returns the empty list if StructFields aren't missing") {

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

      val c = new DataFrameSchemaChecker(sourceDF, requiredSchema)

      assert(c.missingStructFields === List())

    }

  }

  describe("#missingColumnsMessage") {

    it("provides a descriptive message of the StructFields that are missing") {

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

      val c = new DataFrameSchemaChecker(sourceDF, requiredSchema)

      val expected = "The [StructField(name,StringType,true)] StructFields are not included in the DataFrame with the following StructFields [StructType(StructField(num1,IntegerType,true), StructField(num2,IntegerType,true))]"

      assert(c.missingStructFieldsMessage() === expected)

    }

  }

  describe("#validateSchema") {

    it("throws an exception if a required StructField is missing") {

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

      val c = new DataFrameSchemaChecker(sourceDF, requiredSchema)

      intercept[InvalidDataFrameSchemaException] {
        c.validateSchema()
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

      val c = new DataFrameSchemaChecker(sourceDF, requiredSchema)

      c.validateSchema()

    }

  }

}
