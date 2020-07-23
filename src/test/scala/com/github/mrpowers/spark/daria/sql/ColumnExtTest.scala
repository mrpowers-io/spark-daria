package com.github.mrpowers.spark.daria.sql

import utest._

import org.apache.spark.sql.functions._
import ColumnExt._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import SparkSessionExt._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

object ColumnExtTest extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {

    'chain - {

      "chains sql functions" - {

        val df = spark
          .createDF(
            List(
              ("Batman  ", "batman"),
              ("  CATWOMAN", "catwoman"),
              (" pikachu ", "pikachu")
            ),
            List(
              ("word", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "cleaned_word",
            col("word").chain(lower).chain(trim)
          )

        assertColumnEquality(
          df,
          "expected",
          "cleaned_word"
        )

      }

      "chains SQL functions with updated method signatures" - {

        val df = spark
          .createDF(
            List(
              ("hi  ", "hixxx"),
              ("  ok", "okxxx")
            ),
            List(
              ("word", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "diff_word",
            col("word")
              .chain(trim)
              .chain(
                rpad(
                  _,
                  5,
                  "x"
                )
              )
          )

        assertColumnEquality(
          df,
          "expected",
          "diff_word"
        )

      }

    }

    'chainUDF - {

      "allows user defined functions to be chained" - {

        def appendZ(s: String): String = {
          s"${s}Z"
        }

        spark.udf.register(
          "appendZUdf",
          appendZ _
        )

        def prependA(s: String): String = {
          s"A${s}"
        }

        spark.udf.register(
          "prependAUdf",
          prependA _
        )

        val df = spark
          .createDF(
            List(
              ("dance", "AdanceZ"),
              ("sing", "AsingZ")
            ),
            List(
              ("word", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "fun",
            col("word").chainUDF("appendZUdf").chainUDF("prependAUdf")
          )

        assertColumnEquality(
          df,
          "expected",
          "fun"
        )

      }

      "also works with udfs that take arguments" - {

        def appendZ(s: String): String = {
          s"${s}Z"
        }

        spark.udf.register(
          "appendZUdf",
          appendZ _
        )

        def appendWord(s: String, word: String): String = {
          s"${s}${word}"
        }

        spark.udf.register(
          "appendWordUdf",
          appendWord _
        )

        val df = spark
          .createDF(
            List(
              ("dance", "danceZcool"),
              ("sing", "singZcool")
            ),
            List(
              ("word", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "fun",
            col("word")
              .chainUDF("appendZUdf")
              .chainUDF(
                "appendWordUdf",
                lit("cool")
              )
          )

        assertColumnEquality(
          df,
          "expected",
          "fun"
        )

      }

    }

    'chainANDchainUDF - {

      "works with both chain and chainUDF" - {

        def appendZ(s: String): String = {
          s"${s}Z"
        }

        spark.udf.register(
          "appendZUdf",
          appendZ _
        )

        val df = spark
          .createDF(
            List(
              ("Batman  ", "batmanZ"),
              ("  CATWOMAN", "catwomanZ"),
              (" pikachu ", "pikachuZ")
            ),
            List(
              ("word", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "cleaned_word",
            col("word").chain(lower).chain(trim).chainUDF("appendZUdf")
          )

        assertColumnEquality(
          df,
          "expected",
          "cleaned_word"
        )

      }

    }

    'nullBetween - {

      "does a between operation factoring in null values" - {

        val df = spark
          .createDF(
            List(
              (17, null, 94, true),
              (17, null, 10, false),
              (null, 10, 5, true),
              (null, 10, 88, false),
              (10, 15, 11, true),
              (null, null, 11, false),
              (3, 5, null, false),
              (null, null, null, false)
            ),
            List(
              ("lower_age", IntegerType, true),
              ("upper_age", IntegerType, true),
              ("age", IntegerType, true),
              ("expected", BooleanType, true)
            )
          )
          .withColumn(
            "is_between",
            col("age").nullBetween(
              col("lower_age"),
              col("upper_age")
            )
          )

        assertColumnEquality(
          df,
          "expected",
          "is_between"
        )

      }

      "works with joins" - {

        val playersDF = spark.createDF(
          List(
            ("lil", "tball", 5),
            ("strawberry", "mets", 42),
            ("maddux", "braves", 45),
            ("frank", "noteam", null)
          ),
          List(
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
          ),
          List(
            ("lower_age", IntegerType, true),
            ("upper_age", IntegerType, true),
            ("playing_status", StringType, true)
          )
        )

        val actualDF = playersDF
          .join(
            broadcast(rangesDF),
            playersDF("age").nullBetween(
              rangesDF("lower_age"),
              rangesDF("upper_age")
            ),
            "leftouter"
          )
          .drop(
            "lower_age",
            "upper_age"
          )

        val expectedDF = spark.createDF(
          List(
            ("lil", "tball", 5, "too_young"),
            ("strawberry", "mets", 42, "retired"),
            ("maddux", "braves", 45, "retired"),
            ("frank", "noteam", null, null)
          ),
          List(
            ("last_name", StringType, true),
            ("team", StringType, true),
            ("age", IntegerType, true),
            ("playing_status", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "operates differently than the built-in between method" - {

        val sourceDF =
          spark.createDF(
            List(
              (10, 15, 11),
              (17, null, 94),
              (null, 10, 5)
            ),
            List(
              ("lower_bound", IntegerType, true),
              ("upper_bound", IntegerType, true),
              ("age", IntegerType, true)
            )
          )

        val actualDF = sourceDF
          .withColumn(
            "between",
            col("age").between(
              col("lower_bound"),
              col("upper_bound")
            )
          )
          .withColumn(
            "nullBetween",
            col("age").nullBetween(
              col("lower_bound"),
              col("upper_bound")
            )
          )

        val expectedDF = spark.createDF(
          List(
            (10, 15, 11, true, true),
            (17, null, 94, null, true),
            (null, 10, 5, null, true)
          ),
          List(
            ("lower_bound", IntegerType, true),
            ("upper_bound", IntegerType, true),
            ("age", IntegerType, true),
            ("between", BooleanType, true),
            ("nullBetween", BooleanType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'isTrue - {

      "returns true when the column is true" - {

        val df = spark
          .createDF(
            List(
              (true, true),
              (false, false),
              (null, false)
            ),
            List(
              ("is_fun", BooleanType, true),
              ("expected_is_fun_true", BooleanType, true)
            )
          )
          .withColumn(
            "is_fun_true",
            col("is_fun").isTrue
          )

        assertColumnEquality(
          df,
          "expected_is_fun_true",
          "is_fun_true"
        )

      }

    }

    'isFalse - {

      "returns true when the column is false" - {

        val df = spark
          .createDF(
            List(
              (true, false),
              (false, true),
              (null, false)
            ),
            List(
              ("is_fun", BooleanType, true),
              ("expected_is_fun_false", BooleanType, true)
            )
          )
          .withColumn(
            "is_fun_false",
            col("is_fun").isFalse
          )

        assertColumnEquality(
          df,
          "expected_is_fun_false",
          "is_fun_false"
        )

      }

    }

    'isTruthy - {

      "returns true when the column is truthy and false otherwise" - {

        val df = spark
          .createDF(
            List(
              (true, true),
              (false, false),
              (null, false)
            ),
            List(
              ("is_fun", BooleanType, true),
              ("expected", BooleanType, false)
            )
          )
          .withColumn(
            "is_fun_truthy",
            col("is_fun").isTruthy
          )

        assertColumnEquality(
          df,
          "expected",
          "is_fun_truthy"
        )

      }

      "computes a truthy value for string columns" - {

        val df = spark
          .createDF(
            List(
              ("dog", true),
              ("cat", true),
              (null, false)
            ),
            List(
              ("animal_type", StringType, true),
              ("expected", BooleanType, false)
            )
          )
          .withColumn(
            "animal_type_truthy",
            col("animal_type").isTruthy
          )

        assertColumnEquality(
          df,
          "expected",
          "animal_type_truthy"
        )

      }

    }

    'isFalsy - {

      "returns true when the column is falsy and false otherwise" - {

        val df = spark
          .createDF(
            List(
              (true, false),
              (false, true),
              (null, true)
            ),
            List(
              ("is_fun", BooleanType, true),
              ("expected", BooleanType, false)
            )
          )
          .withColumn(
            "is_fun_falsy",
            col("is_fun").isFalsy
          )

        assertColumnEquality(
          df,
          "expected",
          "is_fun_falsy"
        )

      }

      "computes a falsy value for string columns" - {

        val df = spark
          .createDF(
            List(
              ("dog", false),
              ("cat", false),
              (null, true)
            ),
            List(
              ("animal_type", StringType, true),
              ("expected", BooleanType, false)
            )
          )
          .withColumn(
            "animal_type_falsy",
            col("animal_type").isFalsy
          )

        assertColumnEquality(
          df,
          "expected",
          "animal_type_falsy"
        )

      }

    }

    'isNullOrBlank - {

      "returns true if a column is null or blank and false otherwise" - {

        val df = spark
          .createDF(
            List(
              ("dog", false),
              (null, true),
              ("", true),
              ("   ", true)
            ),
            List(
              ("animal_type", StringType, true),
              ("expected", BooleanType, true)
            )
          )
          .withColumn(
            "animal_type_is_null_or_blank",
            col("animal_type").isNullOrBlank
          )

        assertColumnEquality(
          df,
          "expected",
          "animal_type_is_null_or_blank"
        )

      }

    }

    'isNotNullOrBlank - {

      "returns true if a column is not null or blank and false otherwise" - {

        val df = spark
          .createDF(
            List(
              ("notnullhere", true),
              (null, false),
              ("", false),
              ("   ", false)
            ),
            List(
              ("testColumn", StringType, true),
              ("expected", BooleanType, true)
            )
          )
          .withColumn(
            "testColumn_is_not_null_or_blank",
            col("testColumn").isNotNullOrBlank
          )

        assertColumnEquality(
          df,
          "expected",
          "testColumn_is_not_null_or_blank"
        )

      }

    }

    'isNotIn - {

      "returns true if the column element is not in the list" - {

        val footwearRelated = Seq(
          "laces",
          "shoes"
        )

        val df = spark
          .createDF(
            List(
              ("dog", true),
              ("shoes", false),
              ("laces", false),
              (null, null)
            ),
            List(
              ("stuff", StringType, true),
              ("expected", BooleanType, true)
            )
          )
          .withColumn(
            "is_not_footwear_related",
            col("stuff").isNotIn(footwearRelated: _*)
          )

        assertColumnEquality(
          df,
          "expected",
          "is_not_footwear_related"
        )

      }

    }

    'evalString - {
      "lowercases a string" - {
        assert(lower(lit("HI THERE")).evalString() == "hi there")
      }
    }

  }

}
