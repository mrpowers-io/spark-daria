package com.github.mrpowers.spark.daria.sql

import SparkSessionExt._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec

class SparkSessionExtSpec extends FunSpec with DataFrameSuiteBase {

  describe("#createDF") {

    it("creates a DataFrame") {

      val actualDF = spark.createDF(
        List(
          Row(1, 2)
        ), List(
          StructField("num1", IntegerType, true),
          StructField("num2", IntegerType, true)
        )
      )

      val expectedData = List(
        Row(1, 2)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDF)

    }

    it("creates a DataFrame with an array of tuples instead of StructFields") {

      val actualDF = spark.createDF(
        List(
          Row(1, 2)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true)
        )
      )

      val expectedData = List(
        Row(1, 2)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDF)

    }

    it("creates a DataFrame with lists of tuples") {

      val actualDF = spark.createDF(
        List(
          (1, 2)
        ), List(
          ("num1", IntegerType, true),
          ("num2", IntegerType, true)
        )
      )

      val expectedData = List(
        Row(1, 2)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDF)

    }

    it("requires an implicit definition when only the column is present") {

      val namesDF = spark.createDF(
        List(
          ("Alice"),
          ("Bob")
        ), List(
          ("Name", StringType, true)
        )
      )

    }

  }

}
