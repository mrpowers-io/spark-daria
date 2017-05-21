package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import ColumnExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class ColumnExtSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe("#chain") {

    it("chains sql functions") {

      val wordsDF = Seq(
        ("Batman  "),
        ("  CATWOMAN"),
        (" pikachu ")
      ).toDF("word")

      val actualDF = wordsDF.withColumn(
        "cleaned_word",
        col("word").chain(lower).chain(trim)
      )

      val expectedDF = Seq(
        ("Batman  ", "batman"),
        ("  CATWOMAN", "catwoman"),
        (" pikachu ", "pikachu")
      ).toDF("word", "cleaned_word")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("chains SQL functions with updated method signatures") {

      val wordsDF = Seq(
        ("hi  "),
        ("  ok")
      ).toDF("word")

      val actualDF = wordsDF.withColumn(
        "diff_word",
        col("word").chain(trim).chain(functions.rpadDaria(5, "x"))
      )

      val expectedDF = Seq(
        ("hi  ", "hixxx"),
        ("  ok", "okxxx")
      ).toDF("word", "diff_word")

      assertSmallDataFrameEquality(actualDF, expectedDF)

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

      spark.udf.register("prependAUdf", prependA _)

      val hobbiesDF = Seq(
        ("dance"),
        ("sing")
      ).toDF("word")

      val actualDF = hobbiesDF.withColumn(
        "fun",
        col("word").chainUDF("appendZUdf").chainUDF("prependAUdf")
      )

      val expectedDF = Seq(
        ("dance", "AdanceZ"),
        ("sing", "AsingZ")
      ).toDF("word", "fun")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("also works with udfs that take arguments") {

      def appendZ(s: String): String = {
        s"${s}Z"
      }

      spark.udf.register("appendZUdf", appendZ _)

      def appendWord(s: String, word: String): String = {
        s"${s}${word}"
      }

      spark.udf.register("appendWordUdf", appendWord _)

      val hobbiesDF = Seq(
        ("dance"),
        ("sing")
      ).toDF("word")

      val actualDF = hobbiesDF.withColumn(
        "fun",
        col("word").chainUDF("appendZUdf").chainUDF("appendWordUdf", lit("cool"))
      )

      val expectedDF = Seq(
        ("dance", "danceZcool"),
        ("sing", "singZcool")
      ).toDF("word", "fun")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#chain and #chainUDF") {

    it("works with both chain and chainUDF") {

      def appendZ(s: String): String = {
        s"${s}Z"
      }

      spark.udf.register("appendZUdf", appendZ _)

      val wordsDF = Seq(
        ("Batman  "),
        ("  CATWOMAN"),
        (" pikachu ")
      ).toDF("word")

      val actualDF = wordsDF.withColumn(
        "cleaned_word",
        col("word").chain(lower).chain(trim).chainUDF("appendZUdf")
      )

      val expectedDF = Seq(
        ("Batman  ", "batmanZ"),
        ("  CATWOMAN", "catwomanZ"),
        (" pikachu ", "pikachuZ")
      ).toDF("word", "cleaned_word")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
