package com.github.mrpowers.spark.daria.sql

import java.sql.{Date, Timestamp}
import utest._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import SparkSessionExt._
import com.github.mrpowers.spark.daria.sql.functions.arrayConcat

object FunctionsTest extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {

    'singleSpace - {

      "single spaces a string" - {

        val sourceDF = spark.createDF(
          List(
            ("Bruce   willis"),
            ("    obama"),
            ("  nice  hair person  "),
            (null)
          ),
          List(("some_string", StringType, true))
        )

        val actualDF =
          sourceDF.withColumn(
            "some_string_single_spaced",
            functions.singleSpace(col("some_string"))
          )

        val expectedDF = spark.createDF(
          List(
            ("Bruce   willis", "Bruce willis"),
            ("    obama", "obama"),
            ("  nice  hair person  ", "nice hair person"),
            (null, null)
          ),
          List(
            ("some_string", StringType, true),
            ("some_string_single_spaced", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'removeAllWhitespace - {

      "removes all whitespace from a string with a colName argument" - {

        val df = spark
          .createDF(
            List(
              ("Bruce   willis   ", "Brucewillis"),
              ("    obama", "obama"),
              ("  nice  hair person  ", "nicehairperson"),
              (null, null)
            ),
            List(
              ("some_string", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "some_string_without_whitespace",
            functions.removeAllWhitespace("some_string")
          )

        assertColumnEquality(
          df,
          "expected",
          "some_string_without_whitespace"
        )

      }

    }

    'antiTrim - {

      "removes all inner whitespace from a string" - {

        val sourceDF = spark.createDF(
          List(
            ("Bruce   willis   "),
            ("    obama"),
            ("  nice  hair person  "),
            (null)
          ),
          List(("some_string", StringType, true))
        )

        val actualDF =
          sourceDF.withColumn(
            "some_string_anti_trimmed",
            functions.antiTrim(col("some_string"))
          )

        val expectedDF = spark.createDF(
          List(
            ("Bruce   willis   ", "Brucewillis   "),
            ("    obama", "    obama"),
            ("  nice  hair person  ", "  nicehairperson  "),
            (null, null)
          ),
          List(
            ("some_string", StringType, true),
            ("some_string_anti_trimmed", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'removeNonWordCharacters - {

      "removes all non-word characters from a string, excluding whitespace" - {

        val df = spark
          .createDF(
            List(
              ("Bruce &&**||ok88", "Bruce ok88"),
              ("55    oba&&&ma", "55    obama"),
              ("  ni!!ce  h^^air person  ", "  nice  hair person  "),
              (null, null)
            ),
            List(
              ("some_string", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "some_string_remove_non_word_chars",
            functions.removeNonWordCharacters(col("some_string"))
          )

        assertColumnEquality(
          df,
          "some_string_remove_non_word_chars",
          "expected"
        )

      }

    }

    'yeardiff - {
      "calculates the years between two dates" - {
        val testDF = spark.createDF(
          List(
            (Timestamp.valueOf("2016-09-10 00:00:00"), Timestamp.valueOf("2001-08-10 00:00:00")),
            (Timestamp.valueOf("2016-04-18 00:00:00"), Timestamp.valueOf("2010-05-18 00:00:00")),
            (Timestamp.valueOf("2016-01-10 00:00:00"), Timestamp.valueOf("2013-08-10 00:00:00")),
            (null, null)
          ),
          List(
            ("first_datetime", TimestampType, true),
            ("second_datetime", TimestampType, true)
          )
        )
        val actualDF = testDF
          .withColumn(
            "num_years",
            functions.yeardiff(
              col("first_datetime"),
              col("second_datetime")
            )
          )
        val expectedDF = spark.createDF(
          List(
            (15.095890410958905),
            (5.923287671232877),
            (2.419178082191781),
            (null)
          ),
          List(("num_years", DoubleType, true))
        )
        assertSmallDataFrameEquality(
          actualDF.select("num_years"),
          expectedDF
        )
      }
    }

    "dayOfWeekStr" - {
      "converts a dayofweek output to the day of week string representation" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2020-12-28"), "Mon"),
              (Date.valueOf("2021-01-03"), "Sun"),
              (Date.valueOf("2020-12-12"), "Sat"),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn("dayofweek", dayofweek(col("some_date")))
          .withColumn("day_of_week_str", functions.dayOfWeekStr(col("dayofweek")))
        assertColumnEquality(df, "day_of_week_str", "expected")
      }

      "displays a bunch of different options for a Stackoverflow answer" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2021-01-09")),
              (Date.valueOf("2021-01-10")),
              (Date.valueOf("2021-01-11")),
              (Date.valueOf("2021-01-12")),
              (Date.valueOf("2021-01-13")),
              (Date.valueOf("2021-01-14")),
              (Date.valueOf("2021-01-15")),
              (Date.valueOf("2021-01-16")),
              (Date.valueOf("2021-01-17")),
              (Date.valueOf("2021-01-18")),
              (null)
            ),
            List(
              ("some_date", DateType, true)
            )
          )
          .withColumn("beginning_of_week", functions.beginningOfWeek(col("some_date")))
          .withColumn("end_of_week", functions.endOfWeek(col("some_date")))
          .show()

      }
    }

    "beginningOfWeek" - {
      "returns the date at the start of the week using the default week start" - {
        val df = spark
          .createDF(
            List(
              // Remember the default week start is Sunday
              // When Monday, then the Sunday prior
              (Date.valueOf("2020-12-28"), Date.valueOf("2020-12-27")),
              // When Sunday, then the same day
              (Date.valueOf("2021-01-03"), Date.valueOf("2021-01-03")),
              // When Saturday, then the Sunday prior
              (Date.valueOf("2020-12-12"), Date.valueOf("2020-12-06")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("res", functions.beginningOfWeek(col("some_date")))
        assertColumnEquality(df, "res", "expected")
      }

      "returns the date at the start of the week using a custom week start" - {
        val df = spark
          .createDF(
            List(
              // This example considers Tuesday to be the last day of the week
              // So Wednesday is the first day of the week
              // for a Monday, the week start is the Wednesday prior
              (Date.valueOf("2020-12-28"), Date.valueOf("2020-12-23")),
              // for a Sunday, the week start is the Wednesday prior
              (Date.valueOf("2021-01-03"), Date.valueOf("2020-12-30")),
              // for a Saturday, the week start is the Wednesday prior
              (Date.valueOf("2020-12-12"), Date.valueOf("2020-12-09")),
              // for a Wednesday, the week start the same day
              (Date.valueOf("2020-12-23"), Date.valueOf("2020-12-23")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("res", functions.beginningOfWeek(col("some_date"), lastDayOfWeek = "Tue"))
        assertColumnEquality(df, "res", "expected")
      }

      "errors out if the lastDayOfWeek is invalid" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2020-12-28"), Date.valueOf("2020-12-23")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
        val e = intercept[DariaValidationError] {
          df.withColumn("res", functions.beginningOfWeek(col("some_date"), lastDayOfWeek = "Cat"))
        }
      }
    }

    "endOfWeek" - {
      "returns the date at the end of the week using the default week start" - {
        val df = spark
          .createDF(
            List(
              // Remember, Saturday is last day of week by default
              // if Sunday, then next Saturday
              (Date.valueOf("2020-12-27"), Date.valueOf("2021-01-02")),
              // if Monday, then next Saturday
              (Date.valueOf("2020-12-28"), Date.valueOf("2021-01-02")),
              // if Sunday, then next Saturday
              (Date.valueOf("2021-01-03"), Date.valueOf("2021-01-09")),
              // if Saturday, then current day
              (Date.valueOf("2020-12-12"), Date.valueOf("2020-12-12")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("res", functions.endOfWeek(col("some_date")))
        assertColumnEquality(df, "res", "expected")
      }
    }

    "beginningOfMonth" - {
      "returns the date at the start of the month for column argument" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2016-09-10"), Date.valueOf("2016-09-01")),
              (Date.valueOf("2020-01-01"), Date.valueOf("2020-01-01")),
              (Date.valueOf("2016-01-10"), Date.valueOf("2016-01-01")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("res", functions.beginningOfMonthDate(col("some_date")))
        assertColumnEquality(df, "res", "expected")
      }

      "returns the date at the start of the month for column name" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2016-09-10"), Date.valueOf("2016-09-01")),
              (Date.valueOf("2020-01-01"), Date.valueOf("2020-01-01")),
              (Date.valueOf("2016-01-10"), Date.valueOf("2016-01-01")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("res", functions.beginningOfMonth("some_date"))
        assertColumnEquality(df, "res", "expected")
      }

      "works for timestamp type columns" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2017-11-25"), Timestamp.valueOf("2017-11-01 00:00:00")),
              (Date.valueOf("2017-12-21"), Timestamp.valueOf("2017-12-01 00:00:00")),
              (Date.valueOf("2017-09-12"), Timestamp.valueOf("2017-09-01 00:00:00")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", TimestampType, true)
            )
          )
          .withColumn("beginning_of_month_time", functions.beginningOfMonthTime(col("some_date")))
        assertColumnEquality(df, "beginning_of_month_time", "expected")
      }
    }

    "endOfMonth" - {
      "returns the date at the end of the month" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2016-09-10"), Date.valueOf("2016-09-30")),
              (Date.valueOf("2020-01-01"), Date.valueOf("2020-01-31")),
              (Date.valueOf("2016-01-10"), Date.valueOf("2016-01-31")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("res", functions.endOfMonthDate(col("some_date")))
        assertColumnEquality(df, "res", "expected")
      }
    }

    "nextWeekDay" - {
      "calculates the next week day" - {
        val df = spark
          .createDF(
            List(
              (Date.valueOf("2021-01-10"), Date.valueOf("2021-01-11")),
              (Date.valueOf("2021-01-11"), Date.valueOf("2021-01-12")),
              (Date.valueOf("2021-01-12"), Date.valueOf("2021-01-13")),
              (Date.valueOf("2021-01-13"), Date.valueOf("2021-01-14")),
              (Date.valueOf("2021-01-14"), Date.valueOf("2021-01-15")),
              (Date.valueOf("2021-01-15"), Date.valueOf("2021-01-18")),
              (Date.valueOf("2021-01-16"), Date.valueOf("2021-01-18")),
              (null, null)
            ),
            List(
              ("some_date", DateType, true),
              ("expected", DateType, true)
            )
          )
          .withColumn("next_weekday", functions.nextWeekday(col("some_date")))
        assertColumnEquality(df, "next_weekday", "expected")
      }
    }

    //'capitalizeFully - {
    //"uses the supplied delimeter to identify word breaks with org.apache.commons WordUtils.capitalizeFully" - {
    //val df = spark
    //.createDF(
    //List(
    //("Bruce,willis", "Bruce,Willis"),
    //("Trump,donald", "Trump,Donald"),
    //("clinton,Hillary", "Clinton,Hillary"),
    //("Brack obama", "Brack obama"),
    //("george w. bush", "George w. bush"),
    //(null, null)
    //),
    //List(
    //("some_string", StringType, true),
    //("expected", StringType, true)
    //)
    //)
    //.withColumn(
    //"some_string_udf",
    //functions.capitalizeFully(
    //col("some_string"),
    //lit(",")
    //)
    //)
    //assertColumnEquality(
    //df,
    //"expected",
    //"some_string_udf"
    //)
    //}

    //"can be called with multiple delimiters" - {
    //val df = spark
    //.createDF(
    //List(
    //("Bruce,willis", "Bruce,Willis"),
    //("Trump,donald", "Trump,Donald"),
    //("clinton,Hillary", "Clinton,Hillary"),
    //("Brack/obama", "Brack/Obama"),
    //("george w. bush", "George W. Bush"),
    //("RALPHY", "Ralphy"),
    //(null, null)
    //),
    //List(
    //("some_string", StringType, true),
    //("expected", StringType, true)
    //)
    //)
    //.withColumn(
    //"some_string_udf",
    //functions.capitalizeFully(
    //col("some_string"),
    //lit("/, ")
    //)
    //)
    //assertColumnEquality(
    //df,
    //"expected",
    //"some_string_udf"
    //)
    //}
    //}

    'multiEquals - {

      "returns true if all specified input columns satisfy the And condition" - {

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

        val sourceDF = spark.createDF(
          sourceData,
          sourceSchema
        )

        val actualDF = sourceDF.withColumn(
          "valid_flag",
          functions.multiEquals[Boolean](
            true,
            col("c1"),
            col("c2")
          ) &&
            functions.multiEquals[Boolean](
              false,
              col("c3"),
              col("c4")
            )
        )

        val expectedData = List(
          (true, false, true, false, false),
          (false, false, true, false, false),
          (true, true, true, true, false),
          (true, true, false, false, true),
          (true, true, true, false, false)
        )

        val expectedSchema = sourceSchema ::: List(("valid_flag", BooleanType, true))

        val expectedDF = spark.createDF(
          expectedData,
          expectedSchema
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

      "works for strings too" - {

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
          functions.multiEquals[String](
            "cat",
            col("s1"),
            col("s2")
          )
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

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'truncate - {

      "truncates a string" - {

        val sourceDF = spark.createDF(
          List(
            ("happy person"),
            ("fun person"),
            ("laughing person"),
            (null)
          ),
          List(("some_string", StringType, true))
        )

        val actualDF =
          sourceDF.withColumn(
            "some_string_truncated",
            functions.truncate(
              col("some_string"),
              3
            )
          )

        val expectedDF = spark.createDF(
          List(
            ("happy person", "hap"),
            ("fun person", "fun"),
            ("laughing person", "lau"),
            (null, null)
          ),
          List(
            ("some_string", StringType, true),
            ("some_string_truncated", StringType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )

      }

    }

    'arrayExNull - {

      "creates an array excluding null elements" - {

        val sourceDF = spark.createDF(
          List(
            ("a", "b"),
            (null, "b"),
            ("a", null),
            (null, null)
          ),
          List(
            ("c1", StringType, true),
            ("c2", StringType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "mucho_cols",
            functions.arrayExNull(
              col("c1"),
              col("c2")
            )
          )

        val expectedDF = spark.createDF(
          List(
            (
              "a",
              "b",
              Array(
                "a",
                "b"
              )
            ),
            (null, "b", Array("b")),
            ("a", null, Array("a")),
            (null, null, Array[String]())
          ),
          List(
            ("c1", StringType, true),
            ("c2", StringType, true),
            (
              "mucho_cols",
              ArrayType(
                StringType,
                true
              ),
              false
            )
          )
        )

        //        assert(actualDF.collect().deep == expectedDF.collect().deep)
        // HACK
        // NEED TO ADD A TEST HERE WHEN I AM LESS TIRED

      }

    }

    'bucketFinder - {

      "finds what bucket a column value belongs in" - {

        val df = spark
          .createDF(
            List(
              // works for standard use cases
              (24, "20-30"),
              (45, "30-60"),
              // works with range boundries
              (10, "10-20"),
              (20, "10-20"),
              // works with less than / greater than
              (3, "<10"),
              (99, ">70"),
              // works for numbers that don't fall in any buckets
              (65, null),
              // works with null
              (null, null)
            ),
            List(
              ("some_num", IntegerType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "bucket",
            functions.bucketFinder(
              col("some_num"),
              Array(
                (null, 10),
                (10, 20),
                (20, 30),
                (30, 60),
                (70, null)
              ),
              inclusiveBoundries = true
            )
          )

        assertColumnEquality(
          df,
          "expected",
          "bucket"
        )

      }

      "can use inclusive bucket ranges" - {

        val df = spark
          .createDF(
            List(
              // works for standard use cases
              (15, "10-20"),
              // works with range boundries
              (10, "10-20"),
              (20, "10-20"),
              (50, "41-50"),
              (40, "31-40"),
              // works with less than / greater than
              (9, "<10"),
              (72, ">70"),
              // works for numbers that don't fall in any bucket
              (65, null),
              // works with null
              (null, null)
            ),
            List(
              ("some_num", IntegerType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "bucket",
            functions.bucketFinder(
              col("some_num"),
              Array(
                (null, 10),
                (10, 20),
                (21, 30),
                (31, 40),
                (41, 50),
                (70, null)
              ),
              inclusiveBoundries = true
            )
          )

        assertColumnEquality(
          df,
          "expected",
          "bucket"
        )

      }

      "works with a highly customized use case" - {

        val df = spark
          .createDF(
            List(
              (0, "<1"),
              (1, "1-1"),
              (2, "2-4"),
              (3, "2-4"),
              (4, "2-4"),
              (10, "5-74"),
              (75, ">=75"),
              (90, ">=75"),
              (null, null)
            ),
            List(
              ("some_num", IntegerType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "bucket",
            functions.bucketFinder(
              col("some_num"),
              Array(
                (null, 1),
                (1, 1),
                (2, 4),
                (5, 74),
                (75, null)
              ),
              inclusiveBoundries = true,
              lowestBoundLte = false,
              highestBoundGte = true
            )
          )

        assertColumnEquality(
          df,
          "expected",
          "bucket"
        )

      }

      "works with a highly customized use case" - {

        val df = spark
          .createDF(
            List(
              (0, "<1"),
              (10, "1-11"),
              (11, ">=11")
            ),
            List(
              ("some_num", IntegerType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "bucket",
            functions.bucketFinder(
              col("some_num"),
              Array(
                (null, 1),
                (1, 11),
                (11, null)
              ),
              inclusiveBoundries = false,
              highestBoundGte = true
            )
          )

        assertColumnEquality(
          df,
          "expected",
          "bucket"
        )

      }

    }

    'isLuhnNumber - {

      val df = spark
        .createDF(
          List(
            ("49927398716", true),
            ("49927398717", false),
            ("1234567812345678", false),
            ("1234567812345670", true),
            ("808401831202241", true),
            ("", false),
            (null, null)
          ),
          List(
            ("something", StringType, true),
            ("expected", BooleanType, true)
          )
        )
        .withColumn(
          "is_something_luhn",
          functions.isLuhnNumber(col("something"))
        )

      assertColumnEquality(
        df,
        "is_something_luhn",
        "expected"
      )

    }

    'regexp_extract_all - {

      val df = spark
        .createDF(
          List(
            ("this 123 is 456 something", Array("123", "456")),
            ("12", Array("12")),
            ("i like people", Array[String]()),
            ("", Array[String]()),
            (null, null)
          ),
          List(
            ("something", StringType, true),
            ("expected", ArrayType(StringType, true), true)
          )
        )
        .withColumn(
          "something_numbers",
          functions.regexp_extract_all(col("something"), lit("""\d+"""))
        )

      assertColumnEquality(
        df,
        "something_numbers",
        "expected"
      )

    }

    'array_groupBy - {

      "works like the Scala groupBy array method" - {

        val df = spark
          .createDF(
            List(
              (Array("snake", "rat", "cool"), Map(false -> Array("snake", "rat"), true -> Array[String]("cool"))),
              (Array("cat", "crazy"), Map(true -> Array("cat", "crazy"))),
              (null, null)
            ),
            List(
              ("words", ArrayType(StringType, true), true),
              ("expected", MapType(BooleanType, ArrayType(StringType, true)), true)
            )
          )
          .withColumn(
            "groupBy_begin_with_c",
            functions
              .array_groupBy[String]((x: String) => x.startsWith("c"))
              .apply(col("words"))
          )

        assertColumnEquality(
          df,
          "groupBy_begin_with_c",
          "expected"
        )

      }

      "works with a regexp" - {

        val df = spark
          .createDF(
            List(
              (Array("AAAsnake", "BBBrat", "cool"), Map(true -> Array("AAAsnake", "BBBrat"), false -> Array[String]("cool"))),
              (Array("BBBcat", "AAAcrazy"), Map(true -> Array("BBBcat", "AAAcrazy"))),
              (null, null)
            ),
            List(
              ("words", ArrayType(StringType, true), true),
              ("expected", MapType(BooleanType, ArrayType(StringType, true)), true)
            )
          )
          .withColumn(
            "groupBy_begin_with_aaa_or_bbb",
            functions
              .array_groupBy[String]((x: String) => x.matches("""^AAA\w+|^BBB\w+"""))
              .apply(col("words"))
          )

        assertColumnEquality(
          df,
          "groupBy_begin_with_aaa_or_bbb",
          "expected"
        )

      }

    }

    'array_filter_nulls - {

      "removes all nulls from arrays" - {

        val df = spark
          .createDF(
            List(
              (Array("snake", null, "cool"), Array("snake", "cool")),
              (Array("cat", null, null), Array("cat")),
              (Array(null, null), Array[String]()),
              (null, null)
            ),
            List(
              ("words", ArrayType(StringType, true), true),
              ("expected", ArrayType(StringType, true), true)
            )
          )
          .withColumn(
            "words_filtered",
            functions.array_filter_nulls[String]().apply(col("words"))
          )

        assertColumnEquality(
          df,
          "words_filtered",
          "expected"
        )

      }

    }

    //'array_map_ex_null - {

    //"works like the Scala map method on Strings" - {

    //val df = spark
    //.createDF(
    //List(
    //(Array("snake", "rat", "cool"), Array("AAAsnake", "AAArat", "AAAcool")),
    //(Array("cat", null, "crazy"), Array("AAAcat", "AAAcrazy")),
    //(Array(null, null), Array[String]()),
    //(null, null)
    //),
    //List(
    //("words", ArrayType(StringType, true), true),
    //("expected", ArrayType(StringType, true), true)
    //)
    //)
    //.withColumn(
    //"AAA_prepended",
    //functions
    //.array_map_ex_null((x: String) => if (x == null) null else "AAA".concat(x))
    //.apply(col("words"))
    //)

    //assertColumnEquality(
    //df,
    //"AAA_prepended",
    //"expected"
    //)

    //}

    //}

    'broadcastedArrayContains - {

      "returns true if the number is special" - {

        val specialNumbers = spark.sparkContext.broadcast(Array("123", "456"))

        val df = spark
          .createDF(
            List(
              ("123", true),
              ("hi", false),
              (null, null)
            ),
            List(
              ("num", StringType, true),
              ("expected", BooleanType, true)
            )
          )
          .withColumn(
            "is_special_number",
            functions.broadcastArrayContains[String](col("num"), specialNumbers)
          )

        assertColumnEquality(df, "is_special_number", "expected")

      }

    }

    'regexp_extract_all_by_groups - {

      "returns an array of all the matches" - {

        val df = spark
          .createDF(
            List(
              ("H34567ok C88dude", Array(Array("34567", "ok"), Array("88", "dude"))),
              ("B123funny", Array(Array("123", "funny"))),
              ("no match here", Array(Array())),
              (null, null)
            ),
            List(
              ("str", StringType, true),
              ("expected", ArrayType(ArrayType(StringType, true)), true)
            )
          )
          .withColumn(
            "actual",
            functions.regexp_extract_all_by_groups(
              lit("""(\w)(\d+)(\w+)"""),
              col("str"),
              lit(Array(2, 3))
            )
          )

        assertColumnEquality(df, "actual", "expected")

      }

    }

    'regexp_extract_all_by_group - {

      "returns an array of all the matches for a single group" - {

        val df = spark
          .createDF(
            List(
              ("H34567ok C88dude", Array("34567", "88")),
              ("B123funny", Array("123")),
              (null, null)
            ),
            List(
              ("str", StringType, true),
              ("expected", ArrayType(StringType, true), true)
            )
          )
          .withColumn(
            "actual",
            functions.regexp_extract_all_by_group(
              lit("""(\w)(\d+)(\w+)"""),
              col("str"),
              lit(2)
            )
          )

        assertColumnEquality(df, "actual", "expected")

      }

    }

    'excelEpochToUnixTimestamp - {

      "convert an Excel epoch time to unix timestamp" - {

        val sourceDF = spark.createDF(
          List(
            (43967.241666666664),
            (33966.783333333776),
            (43965.583833632439),
            (33964.583935339336)
          ),
          List(
            ("excel_time", DoubleType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "unix_timestamp",
            functions.excelEpochToUnixTimestamp(col("excel_time"))
          )

        val expectedDF = spark.createDF(
          List(
            (43967.241666666664, 1.5896080799999995e9),
            (33966.783333333776, 7.255684800000383e8),
            (43965.583833632439, 1.5894648432258427e9),
            (33964.583935339336, 7.253784520133189e8)
          ),
          List(
            ("excel_time", DoubleType, true),
            ("unix_timestamp", DoubleType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'excelEpochToUnixTimestamp - {

      "convert an Excel epoch time to unix timestamp" - {

        val sourceDF = spark.createDF(
          List(
            (43967.241666666664),
            (33966.783333333776),
            (43965.583833632439),
            (33964.583935339336)
          ),
          List(
            ("excel_time", DoubleType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "unix_timestamp",
            functions.excelEpochToUnixTimestamp("excel_time")
          )

        val expectedDF = spark.createDF(
          List(
            (43967.241666666664, 1.5896080799999995e9),
            (33966.783333333776, 7.255684800000383e8),
            (43965.583833632439, 1.5894648432258427e9),
            (33964.583935339336, 7.253784520133189e8)
          ),
          List(
            ("excel_time", DoubleType, true),
            ("unix_timestamp", DoubleType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'excelEpochToTimestamp - {

      "convert an Excel epoch time to timestamp" - {

        val sourceDF = spark.createDF(
          List(
            (43967.241666666664),
            (33966.783333333776),
            (43965.583833632439),
            (33964.583935339336)
          ),
          List(
            ("excel_time", DoubleType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "timestamp",
            functions.excelEpochToTimestamp(col("excel_time"))
          )

        val expectedDF = spark.createDF(
          List(
            (43967.241666666664, Timestamp.valueOf("2020-05-16 05:47:59")),
            (33966.783333333776, Timestamp.valueOf("1992-12-28 18:48:00")),
            (43965.583833632439, Timestamp.valueOf("2020-05-14 14:00:43")),
            (33964.583935339336, Timestamp.valueOf("1992-12-26 14:00:52"))
          ),
          List(
            ("excel_time", DoubleType, true),
            ("timestamp", TimestampType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'excelEpochToTimestamp - {

      "convert an Excel epoch time to timestamp" - {

        val sourceDF = spark.createDF(
          List(
            (43967.241666666664),
            (33966.783333333776),
            (43965.583833632439),
            (33964.583935339336)
          ),
          List(
            ("excel_time", DoubleType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "timestamp",
            functions.excelEpochToTimestamp("excel_time")
          )

        val expectedDF = spark.createDF(
          List(
            (43967.241666666664, Timestamp.valueOf("2020-05-16 05:47:59")),
            (33966.783333333776, Timestamp.valueOf("1992-12-28 18:48:00")),
            (43965.583833632439, Timestamp.valueOf("2020-05-14 14:00:43")),
            (33964.583935339336, Timestamp.valueOf("1992-12-26 14:00:52"))
          ),
          List(
            ("excel_time", DoubleType, true),
            ("timestamp", TimestampType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'excelEpochToDate - {

      "convert an Excel epoch time to date" - {

        val sourceDF = spark.createDF(
          List(
            (43967.241666666664),
            (33966.783333333776),
            (43965.583833632439),
            (33964.583935339336)
          ),
          List(
            ("excel_time", DoubleType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "date",
            functions.excelEpochToDate(col("excel_time"))
          )

        val expectedDF = spark.createDF(
          List(
            (43967.241666666664, Date.valueOf("2020-05-16")),
            (33966.783333333776, Date.valueOf("1992-12-28")),
            (43965.583833632439, Date.valueOf("2020-05-14")),
            (33964.583935339336, Date.valueOf("1992-12-26"))
          ),
          List(
            ("excel_time", DoubleType, true),
            ("date", DateType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'excelEpochToDate - {

      "convert an Excel epoch time to date" - {

        val sourceDF = spark.createDF(
          List(
            (43967.241666666664),
            (33966.783333333776),
            (43965.583833632439),
            (33964.583935339336)
          ),
          List(
            ("excel_time", DoubleType, true)
          )
        )

        val actualDF =
          sourceDF.withColumn(
            "date",
            functions.excelEpochToDate("excel_time")
          )

        val expectedDF = spark.createDF(
          List(
            (43967.241666666664, Date.valueOf("2020-05-16")),
            (33966.783333333776, Date.valueOf("1992-12-28")),
            (43965.583833632439, Date.valueOf("2020-05-14")),
            (33964.583935339336, Date.valueOf("1992-12-26"))
          ),
          List(
            ("excel_time", DoubleType, true),
            ("date", DateType, true)
          )
        )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    "arrayConcat" - {
      "arrayConcat array of string type" - {

        val actualDF = spark
          .createDF(
            List(
              Array(
                "snake",
                "rat"
              ),
              null,
              Array(
                "cat",
                "crazy"
              )
            ),
            List(("array", ArrayType(StringType), true))
          )
          .agg(arrayConcat(col("array")).as("array"))

        val expectedDF = spark
          .createDF(
            List(
              Array(
                "snake",
                "rat",
                "cat",
                "crazy"
              )
            ),
            List(("array", ArrayType(StringType), true))
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF,
          ignoreNullable = true
        )

      }

      "arrayConcat array of Int type" - {

        val actualDF = spark
          .createDF(
            List(
              Array(
                1,
                2
              ),
              null,
              Array(
                3,
                4
              )
            ),
            List(("array", ArrayType(IntegerType), true))
          )
          .agg(arrayConcat(col("array")).as("array"))

        val expectedDF = spark
          .createDF(
            List(Array(1, 2, 3, 4)),
            List(("array", ArrayType(IntegerType), true))
          )

        assertSmallDataFrameEquality(
          actualDF,
          expectedDF,
          ignoreNullable = true
        )

      }

    }

  }
}
