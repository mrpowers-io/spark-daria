package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import ColumnExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import SparkSessionExt._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

class ColumnExtSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe("#chain") {

    it("chains sql functions") {

      val wordsDF = spark.createDF(
        List(
          ("Batman  "),
          ("  CATWOMAN"),
          (" pikachu ")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn(
        "cleaned_word",
        col("word").chain(lower).chain(trim)
      )

      val expectedDF = spark.createDF(
        List(
          ("Batman  ", "batman"),
          ("  CATWOMAN", "catwoman"),
          (" pikachu ", "pikachu")
        ), List(
          ("word", StringType, true),
          ("cleaned_word", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("chains SQL functions with updated method signatures") {

      val wordsDF = spark.createDF(
        List(
          ("hi  "),
          ("  ok")
        ), List(
          ("word", StringType, true)
        )
      )

      val actualDF = wordsDF.withColumn(
        "diff_word",
        col("word").chain(trim).chain(functions.rpadDaria(5, "x"))
      )

      val expectedDF = spark.createDF(
        List(
          ("hi  ", "hixxx"),
          ("  ok", "okxxx")
        ), List(
          ("word", StringType, true),
          ("diff_word", StringType, true)
        )
      )

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

  describe(".nullBetween") {

    it("does a between operation factoring in null values") {

      val sourceDF = spark.createDF(
        List(
          (17, null, 94),
          (17, null, 10),
          (null, 10, 5),
          (null, 10, 88),
          (10, 15, 11),
          (null, null, 11),
          (3, 5, null),
          (null, null, null)
        ), List(
          ("lower_age", IntegerType, true),
          ("upper_age", IntegerType, true),
          ("age", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "is_between",
        col("age").nullBetween(col("lower_age"), col("upper_age"))
      )

      val expectedDF = spark.createDF(
        List(
          (17, null, 94, true),
          (17, null, 10, false),
          (null, 10, 5, true),
          (null, 10, 88, false),
          (10, 15, 11, true),
          (null, null, 11, false),
          (3, 5, null, false),
          (null, null, null, false)
        ), List(
          ("lower_age", IntegerType, true),
          ("upper_age", IntegerType, true),
          ("age", IntegerType, true),
          ("is_between", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("works with joins") {

      val playersDF = spark.createDF(
        List(
          ("lil", "tball", 5),
          ("strawberry", "mets", 42),
          ("maddux", "braves", 45),
          ("frank", "noteam", null)
        ), List(
          ("last_name", StringType, true),
          ("team", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val rangesDF = spark.createDF(
        List(
          (null, 20, "too_young"),
          (21, 40, "prime"),
          (41, null, "retired")
        ), List(
          ("lower_age", IntegerType, true),
          ("upper_age", IntegerType, true),
          ("playing_status", StringType, true)
        )
      )

      val actualDF = playersDF.join(
        broadcast(rangesDF),
        playersDF("age").nullBetween(
          rangesDF("lower_age"),
          rangesDF("upper_age")
        ),
        "leftouter"
      ).drop(
          "lower_age",
          "upper_age"
        )

      val expectedDF = spark.createDF(
        List(
          ("lil", "tball", 5, "too_young"),
          ("strawberry", "mets", 42, "retired"),
          ("maddux", "braves", 45, "retired"),
          ("frank", "noteam", null, null)
        ), List(
          ("last_name", StringType, true),
          ("team", StringType, true),
          ("age", IntegerType, true),
          ("playing_status", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("operates differently than the built-in between method") {

      val sourceDF = spark.createDF(
        List(
          (10, 15, 11),
          (17, null, 94),
          (null, 10, 5)
        ), List(
          ("lower_bound", IntegerType, true),
          ("upper_bound", IntegerType, true),
          ("age", IntegerType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "between",
        col("age").between(col("lower_bound"), col("upper_bound"))
      ).withColumn(
          "nullBetween",
          col("age").nullBetween(col("lower_bound"), col("upper_bound"))
        )

      val expectedDF = spark.createDF(
        List(
          (10, 15, 11, true, true),
          (17, null, 94, null, true),
          (null, 10, 5, null, true)
        ), List(
          ("lower_bound", IntegerType, true),
          ("upper_bound", IntegerType, true),
          ("age", IntegerType, true),
          ("between", BooleanType, true),
          ("nullBetween", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
