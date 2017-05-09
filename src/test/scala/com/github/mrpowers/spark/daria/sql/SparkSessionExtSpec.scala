package com.github.mrpowers.spark.daria.sql

import SparkSessionExt._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
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

  }

}
