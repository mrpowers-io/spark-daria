package com.github.mrpowers.spark.daria.sql

import java.sql.Timestamp

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import SparkSessionExt._

class FunctionsSpec
    extends FunSpec
    with DataFrameComparer
    with ColumnComparer
    with SparkSessionTestWrapper {

  describe("#singleSpace") {

    it("single spaces a string") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce   willis"),
          ("    obama"),
          ("  nice  hair person  "),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "some_string_single_spaced",
        functions.singleSpace(col("some_string"))
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce   willis", "Bruce willis"),
          ("    obama", "obama"),
          ("  nice  hair person  ", "nice hair person"),
          (null, null)
        ), List(
          ("some_string", StringType, true),
          ("some_string_single_spaced", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#removeAllWhitespace") {

    it("removes all whitespace from a string with a column argument") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce   willis   "),
          ("    obama"),
          ("  nice  hair person  "),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "some_string_without_whitespace",
        functions.removeAllWhitespace(col("some_string"))
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce   willis   ", "Brucewillis"),
          ("    obama", "obama"),
          ("  nice  hair person  ", "nicehairperson"),
          (null, null)
        ), List(
          ("some_string", StringType, true),
          ("some_string_without_whitespace", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("removes all whitespace from a string with a colName argument") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce   willis   "),
          ("    obama"),
          ("  nice  hair person  "),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "some_string_without_whitespace",
        functions.removeAllWhitespace("some_string")
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce   willis   ", "Brucewillis"),
          ("    obama", "obama"),
          ("  nice  hair person  ", "nicehairperson"),
          (null, null)
        ), List(
          ("some_string", StringType, true),
          ("some_string_without_whitespace", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#antiTrim") {

    it("removes all inner whitespace from a string") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce   willis   "),
          ("    obama"),
          ("  nice  hair person  "),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "some_string_anti_trimmed",
        functions.antiTrim(col("some_string"))
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce   willis   ", "Brucewillis   "),
          ("    obama", "    obama"),
          ("  nice  hair person  ", "  nicehairperson  "),
          (null, null)
        ), List(
          ("some_string", StringType, true),
          ("some_string_anti_trimmed", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#removeNonWordCharacters") {

    it("removes all non-word characters from a string, excluding whitespace") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce &&**||ok"),
          ("    oba&&&ma"),
          ("  ni!!ce  h^^air person  "),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "some_string_remove_non_word_chars",
        functions.removeNonWordCharacters(col("some_string"))
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce &&**||ok", "Bruce ok"),
          ("    oba&&&ma", "    obama"),
          ("  ni!!ce  h^^air person  ", "  nice  hair person  "),
          (null, null)
        ), List(
          ("some_string", StringType, true),
          ("some_string_remove_non_word_chars", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#convertArrayToStringWithEscape") {

    it("convert Array to String with Special character escape") {

      val origArray = Array("D/E", "(E/F)", "", "E|G", null, "E;;G", "^AB-C")

      val actualStr =
        functions.convertArrayToStringWithEscape(origArray)

      val expectedStr = """D/E|\(E/F\)||E\|G|E;;G|^AB-C"""

      assertResult(actualStr) { expectedStr }

    }

  }

  describe("#yeardiff") {

    it("calculates the years between two dates") {

      val testDF = spark.createDF(
        List(
          (Timestamp.valueOf("2016-09-10 00:00:00"), Timestamp.valueOf("2001-08-10 00:00:00")),
          (Timestamp.valueOf("2016-04-18 00:00:00"), Timestamp.valueOf("2010-05-18 00:00:00")),
          (Timestamp.valueOf("2016-01-10 00:00:00"), Timestamp.valueOf("2013-08-10 00:00:00")),
          (null, null)
        ), List(
          ("first_datetime", TimestampType, true),
          ("second_datetime", TimestampType, true)
        )
      )

      val actualDF = testDF
        .withColumn(
          "num_years",
          functions.yeardiff(col("first_datetime"), col("second_datetime"))
        )

      val expectedDF = spark.createDF(
        List(
          (15.095890410958905),
          (5.923287671232877),
          (2.419178082191781),
          (null)
        ), List(
          ("num_years", DoubleType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF.select("num_years"), expectedDF)

    }

  }

  describe("#capitalizeFully") {

    it("uses the supplied delimeter to identify word breaks with org.apache.commons WordUtils.capitalizeFully") {

      val sourceDF = spark.createDF(
        List(
          ("Bruce,willis"),
          ("Trump,donald"),
          ("clinton,Hillary"),
          ("Brack obama"),
          ("george w. bush"),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.select(
        functions.capitalizeFully(List(','))(col("some_string")).as("some_string_udf")
      )

      val expectedDF = spark.createDF(
        List(
          ("Bruce,Willis"),
          ("Trump,Donald"),
          ("Clinton,Hillary"),
          ("Brack obama"),
          ("George w. bush"),
          (null)
        ), List(
          ("some_string_udf", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#exists") {

    it("returns true if the array includes a value that makes the function return true") {

      val sourceDF = spark.createDF(
        List(
          (Array(1, 4, 9)),
          (Array(1, 3, 5))
        ), List(
          ("nums", ArrayType(IntegerType, true), true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "nums_has_even",
        functions.exists[Int]((x: Int) => x % 2 == 0).apply(col("nums"))
      )

      val expectedDF = spark.createDF(
        List(
          (Array(1, 4, 9), true),
          (Array(1, 3, 5), false)
        ), List(
          ("nums", ArrayType(IntegerType, true), true),
          ("nums_has_even", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#forall") {

    it("works like the Scala forall method") {

      val sourceDF = spark.createDF(
        List(
          (Array("snake", "rat")),
          (Array("cat", "crazy"))
        ), List(
          ("words", ArrayType(StringType, true), true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "all_words_begin_with_c",
        functions.forall[String]((x: String) => x.startsWith("c")).apply(col("words"))
      )

      val expectedDF = spark.createDF(
        List(
          (Array("snake", "rat"), false),
          (Array("cat", "crazy"), true)
        ), List(
          ("words", ArrayType(StringType, true), true),
          ("all_words_begin_with_c", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("multiEquals") {

    it("returns true if all specified input columns satisfy the And condition") {

      val sourceData = List(
        (true, false, true, false),
        (false, false, true, false),
        (true, true, true, true),
        (true, true, false, false),
        (true, true, true, false)
      )

      val sourceSchema = List(
        ("c1", BooleanType, true),
        ("c2", BooleanType, true),
        ("c3", BooleanType, true),
        ("c4", BooleanType, true)
      )

      val sourceDF = spark.createDF(sourceData, sourceSchema)

      val actualDF = sourceDF.withColumn(
        "valid_flag",
        functions.multiEquals[Boolean](true, col("c1"), col("c2")) &&
          functions.multiEquals[Boolean](false, col("c3"), col("c4"))
      )

      val expectedData = List(
        (true, false, true, false, false),
        (false, false, true, false, false),
        (true, true, true, true, false),
        (true, true, false, false, true),
        (true, true, true, false, false)
      )

      val expectedSchema = sourceSchema ::: List(("valid_flag", BooleanType, true))

      val expectedDF = spark.createDF(expectedData, expectedSchema)

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("works for strings too") {

      val sourceDF = spark.createDF(
        List(
          ("cat", "cat"),
          ("cat", "dog"),
          ("pig", "pig"),
          ("", ""),
          (null, null)
        ),
        List(
          ("s1", StringType, true),
          ("s2", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "are_s1_and_s2_cat",
        functions.multiEquals[String]("cat", col("s1"), col("s2"))
      )

      val expectedDF = spark.createDF(
        List(
          ("cat", "cat", true),
          ("cat", "dog", false),
          ("pig", "pig", false),
          ("", "", false),
          (null, null, null)
        ),
        List(
          ("s1", StringType, true),
          ("s2", StringType, true),
          ("are_s1_and_s2_cat", BooleanType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#truncate") {

    it("truncates a string") {

      val sourceDF = spark.createDF(
        List(
          ("happy person"),
          ("fun person"),
          ("laughing person"),
          (null)
        ), List(
          ("some_string", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "some_string_truncated",
        functions.truncate(col("some_string"), 3)
      )

      val expectedDF = spark.createDF(
        List(
          ("happy person", "hap"),
          ("fun person", "fun"),
          ("laughing person", "lau"),
          (null, null)
        ), List(
          ("some_string", StringType, true),
          ("some_string_truncated", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("arrayExNull") {

    it("creates an array excluding null elements") {

      val sourceDF = spark.createDF(
        List(
          ("a", "b"),
          (null, "b"),
          ("a", null),
          (null, null)
        ), List(
          ("c1", StringType, true),
          ("c2", StringType, true)
        )
      )

      val actualDF = sourceDF.withColumn(
        "mucho_cols",
        functions.arrayExNull(col("c1"), col("c2"))
      )

      val expectedDF = spark.createDF(
        List(
          ("a", "b", Array("a", "b")),
          (null, "b", Array("b")),
          ("a", null, Array("a")),
          (null, null, Array[String]())
        ), List(
          ("c1", StringType, true),
          ("c2", StringType, true),
          ("mucho_cols", ArrayType(StringType, true), false)
        )
      )

      actualDF.collect().deep.equals(expectedDF.collect().deep)

    }

  }

}
