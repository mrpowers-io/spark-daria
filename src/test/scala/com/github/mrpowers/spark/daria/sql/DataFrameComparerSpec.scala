package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

class DataFrameComparerSpec extends FunSpec with DataFrameSuiteBase with DataFrameComparer {

  import spark.implicits._

  describe("#assertSmallDataFrameEquality") {

    it("does nothing true if the DataFrames have the same schemas and content") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1),
        (5)
      ).toDF("number")

      assertSmallDataFrameEquality(sourceDF, expectedDF)

    }

    it("throws an error if the DataFrames have the different schemas") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (1, "word"),
        (5, "word")
      ).toDF("number", "word")

      intercept[DataFrameSchemaMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }

    }

    it("returns false if the DataFrames content is different") {

      val sourceDF = Seq(
        (1),
        (5)
      ).toDF("number")

      val expectedDF = Seq(
        (10),
        (5)
      ).toDF("number")

      intercept[DataFrameContentMismatch] {
        assertSmallDataFrameEquality(sourceDF, expectedDF)
      }

    }

  }

}
