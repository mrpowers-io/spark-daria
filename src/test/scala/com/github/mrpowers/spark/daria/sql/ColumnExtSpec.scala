package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import ColumnExt._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import SparkSessionExt._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

class ColumnExtSpec
    extends FunSpec
    with DataFrameComparer
    with ColumnComparer
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
        col("word").chain(trim).chain(rpad(_, 5, "x"))
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

  describe("#isTrue") {

    it("returns true when the column is true") {

      val df = spark.createDF(
        List(
          (true, true),
          (false, false),
          (null, false)
        ), List(
          ("is_fun", BooleanType, true),
          ("expected_is_fun_true", BooleanType, true)
        )
      ).withColumn(
          "is_fun_true",
          col("is_fun").isTrue
        )

      assertColumnEquality(df, "expected_is_fun_true", "is_fun_true")

    }

  }

  describe("#isFalse") {

    it("returns true when the column is false") {

      val df = spark.createDF(
        List(
          (true, false),
          (false, true),
          (null, false)
        ), List(
          ("is_fun", BooleanType, true),
          ("expected_is_fun_false", BooleanType, true)
        )
      ).withColumn(
          "is_fun_false",
          col("is_fun").isFalse
        )

      assertColumnEquality(df, "expected_is_fun_false", "is_fun_false")

    }

  }

  describe("#isTruthy") {

    it("returns true when the column is truthy and false otherwise") {

      val sourceDF = spark.createDF(
        List(
          (true),
          (false),
          (null)
        ), List(
          ("is_fun", BooleanType, true)
        )
      )

      val actualDF = sourceDF.withColumn("is_fun_truthy", col("is_fun").isTruthy)

      val expectedDF = spark.createDF(
        List(
          (true, true),
          (false, false),
          (null, false)
        ), List(
          ("is_fun", BooleanType, true),
          ("is_fun_truthy", BooleanType, false)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("computes a truthy value for string columns") {

      val sourceDF = spark.createDF(
        List(
          ("dog"),
          ("cat"),
          (null)
        ), List(
          ("animal_type", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn("animal_type_truthy", col("animal_type").isTruthy)

      val expectedDF = spark.createDF(
        List(
          ("dog", true),
          ("cat", true),
          (null, false)
        ), List(
          ("animal_type", StringType, true),
          ("animal_type_truthy", BooleanType, false)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#isFalsy") {

    it("returns true when the column is falsy and false otherwise") {

      val sourceDF = spark.createDF(
        List(
          (true),
          (false),
          (null)
        ), List(
          ("is_fun", BooleanType, true)
        )
      )

      val actualDF = sourceDF.withColumn("is_fun_falsy", col("is_fun").isFalsy)

      val expectedDF = spark.createDF(
        List(
          (true, false),
          (false, true),
          (null, true)
        ), List(
          ("is_fun", BooleanType, true),
          ("is_fun_falsy", BooleanType, false)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("computes a falsy value for string columns") {

      val sourceDF = spark.createDF(
        List(
          ("dog"),
          ("cat"),
          (null)
        ), List(
          ("animal_type", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "animal_type_falsy",
        col("animal_type").isFalsy
      )

      val expectedDF = spark.createDF(
        List(
          ("dog", false),
          ("cat", false),
          (null, true)
        ), List(
          ("animal_type", StringType, true),
          ("animal_type_falsy", BooleanType, false)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#isNullOrBlank") {

    it("returns true if a column is null or blank and false otherwise") {

      val sourceDF = spark.createDF(
        List(
          ("dog"),
          (null),
          (""),
          ("   ")
        ), List(
          ("animal_type", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "animal_type_is_null_or_blank",
        col("animal_type").isNullOrBlank
      )

      val expectedDF = spark.createDF(
        List(
          ("dog", false),
          (null, true),
          ("", true),
          ("   ", true)
        ), List(
          ("animal_type", StringType, true),
          ("animal_type_is_null_or_blank", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#isNotNullOrBlank") {

    it("returns true if a column is not null or blank and false otherwise") {

      val sourceDF = spark.createDF(
        List(
          "notnullhere",
          null,
          "",
          "   "
        ), List(
          ("testColumn", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "testColumn_is_not_null_or_blank",
        col("testColumn").isNotNullOrBlank
      )

      val expectedDF = spark.createDF(
        List(
          ("notnullhere", true),
          (null, false),
          ("", false),
          ("   ", false)
        ), List(
          ("testColumn", StringType, true),
          ("testColumn_is_not_null_or_blank", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#isNotIn") {

    it("returns true if the column element is not in the list") {

      val sourceDF = spark.createDF(
        List(
          ("dog"),
          ("shoes"),
          ("laces"),
          (null)
        ), List(
          ("stuff", StringType, true)
        )
      )

      val footwearRelated = Seq("laces", "shoes")

      val actualDF = sourceDF.withColumn(
        "is_not_footwear_related",
        col("stuff").isNotIn(footwearRelated: _*)
      )

      val expectedDF = spark.createDF(
        List(
          ("dog", true),
          ("shoes", false),
          ("laces", false),
          (null, null)
        ), List(
          ("stuff", StringType, true),
          ("is_not_footwear_related", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
