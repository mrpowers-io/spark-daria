package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSpec

class SparkSessionExtSpec extends FunSpec with DataFrameSuiteBase {

  describe("#createDF") {

    it("creates a DataFrame with a list of Row and List of StructField") {

      val actualDF = spark.createDF(
        List(
          Row(1, 2)
        ),
        List(
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

    it(
      "creates a DataFrame with an List of Row and Tuples instead of StructFields"
    ) {

        val actualDF = spark.createDF(
          List(
            Row(1, 2)
          ),
          List(
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

    it("creates a DataFrame with lists of tuples for data and StructFields") {

      val actualDF = spark.createDF(
        List(
          (1, 2)
        ),
        List(
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

    it("creates a Dataframe with a list of tuples for both values and StructFields") {

      val actualDF = spark.createDF(
        List(
          (1, 2)
        ),
        List(
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

    it("creates a Dataframe with a list of primitives for data and tuples of struct fields") {

      val actualDF = spark.createDF(
        List(
          1,
          2
        ),
        List(
          ("num1", IntegerType, true)
        )
      )

      val expectedData = List(
        Row(1),
        Row(2)
      )

      val expectedSchema = List(
        StructField("num1", IntegerType, true)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertDataFrameEquals(actualDF, expectedDF)

    }
  }

}
