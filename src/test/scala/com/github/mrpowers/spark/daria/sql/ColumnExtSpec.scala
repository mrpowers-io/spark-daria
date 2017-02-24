package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

import org.apache.spark.sql.functions._

import ColumnExt._

class ColumnExtSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("#chain") {

    it("chains sql functions") {

      val wordsDf = Seq(
        ("Batman  "),
        ("  CATWOMAN"),
        (" pikachu ")
      ).toDF("word")

      val actualDf = wordsDf.withColumn("cleaned_word", col("word").chain(lower).chain(trim))

      val expectedDf = Seq(
        ("Batman  ", "batman"),
        ("  CATWOMAN", "catwoman"),
        (" pikachu ", "pikachu")
      ).toDF("word", "cleaned_word")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#chainUDF") {

    it("allows user defined functions to be chained") {

      def appendZ(s: String): String = {
        s"${s}Z"
      }

      spark.udf.register("appendZUdf", appendZ _)

      def prependA(s: String): String = {
        s"A${s}"
      }

      spark.udf.register("prependA", prependA _)

      val hobbiesDf = Seq(
        ("dance"),
        ("sing")
      ).toDF("word")

      val actualDf = hobbiesDf.withColumn("fun", col("word").chainUDF("appendZUdf").chainUDF("prependA"))

      val expectedDf = Seq(
        ("dance", "AdanceZ"),
        ("sing", "AsingZ")
      ).toDF("word", "fun")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

}
