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

      val actualDf = wordsDf.withColumn(
        "cleaned_word",
        col("word").chain(lower).chain(trim)
      )

      val expectedDf = Seq(
        ("Batman  ", "batman"),
        ("  CATWOMAN", "catwoman"),
        (" pikachu ", "pikachu")
      ).toDF("word", "cleaned_word")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("chains SQL functions with updated method signatures") {

      val wordsDf = Seq(
        ("hi  "),
        ("  ok")
      ).toDF("word")

      val actualDf = wordsDf.withColumn(
        "diff_word",
        col("word").chain(trim).chain(functions.rpadDaria(5, "x"))
      )

      val expectedDf = Seq(
        ("hi  ", "hixxx"),
        ("  ok", "okxxx")
      ).toDF("word", "diff_word")

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

      spark.udf.register("prependAUdf", prependA _)

      val hobbiesDf = Seq(
        ("dance"),
        ("sing")
      ).toDF("word")

      val actualDf = hobbiesDf.withColumn(
        "fun",
        col("word").chainUDF("appendZUdf").chainUDF("prependAUdf")
      )

      val expectedDf = Seq(
        ("dance", "AdanceZ"),
        ("sing", "AsingZ")
      ).toDF("word", "fun")

      assertDataFrameEquals(actualDf, expectedDf)

    }

    it("works with udfs that take arguments too") {

      def appendZ(s: String): String = {
        s"${s}Z"
      }

      spark.udf.register("appendZUdf", appendZ _)

      def appendWord(s: String, word: String): String = {
        s"${s}${word}"
      }

      spark.udf.register("appendWordUdf", appendWord _)

      val hobbiesDf = Seq(
        ("dance"),
        ("sing")
      ).toDF("word")

      val actualDf = hobbiesDf.withColumn(
        "fun",
        col("word").chainUDF("appendZUdf").chainUDF("appendWordUdf", lit("cool"))
      )

      val expectedDf = Seq(
        ("dance", "danceZcool"),
        ("sing", "singZcool")
      ).toDF("word", "fun")

      assertDataFrameEquals(actualDf, expectedDf)

    }

  }

  describe("#chain and #chainUDF") {

   it("works with both chain and chainUDF") {

     def appendZ(s: String): String = {
       s"${s}Z"
     }

     spark.udf.register("appendZUdf", appendZ _)

     val wordsDf = Seq(
       ("Batman  "),
       ("  CATWOMAN"),
       (" pikachu ")
     ).toDF("word")

     val actualDf = wordsDf.withColumn(
       "cleaned_word",
       col("word").chain(lower).chain(trim).chainUDF("appendZUdf")
     )

     val expectedDf = Seq(
       ("Batman  ", "batmanZ"),
       ("  CATWOMAN", "catwomanZ"),
       (" pikachu ", "pikachuZ")
     ).toDF("word", "cleaned_word")

     assertDataFrameEquals(actualDf, expectedDf)

   }

  }

}
