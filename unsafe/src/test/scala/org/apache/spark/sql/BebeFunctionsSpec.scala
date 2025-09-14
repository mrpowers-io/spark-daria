package org.apache.spark.sql

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import utest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.BebeFunctions._
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.types._

import java.time.ZonedDateTime
import java.util.TimeZone
import java.time.ZoneId
import java.time.Instant

object BebeFunctionsSpec extends TestSuite with SparkSessionTestWrapper with ColumnComparer with DataFrameComparer {
  import spark.implicits._

  // ADDITIONAL HELPER FUNCTIONS
  private val regex = s"([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})".r

  private def valueOf(date: String, zone: String): Instant = {
    date match {
      case regex(year, month, day, hour, minute, second) =>
        ZonedDateTime
          .of(
            year.toInt,
            month.toInt,
            day.toInt,
            hour.toInt,
            minute.toInt,
            second.toInt,
            0,
            ZoneId.of(zone)
          )
          .toInstant()
    }
  }

  override def tests =
    Tests {
      "beginningOfDay" - {
        "returns the beginning of the day" - {
          val df = Seq(
            (Timestamp.valueOf("2020-01-15 08:01:32"), Timestamp.valueOf("2020-01-15 00:00:00")),
            (Timestamp.valueOf("2020-01-20 23:03:22"), Timestamp.valueOf("2020-01-20 00:00:00")),
            (null, null)
          ).toDF("some_time", "expected")
          df.select("some_time").show()
          val resDF = df
            .withColumn("actual", beginningOfDay(col("some_time")))
          resDF.select("some_time", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "returns the beginning of the day in a specific timezone" - {
          val testTimezone         = "America/Bahia"
          val defaultTimezone      = TimeZone.getDefault()
          val sparkDefaultTimezone = spark.conf.get("spark.sql.session.timeZone")
          println(defaultTimezone.getDisplayName())
          println(sparkDefaultTimezone)
          TimeZone.setDefault(TimeZone.getTimeZone(testTimezone))
          spark.conf.set("spark.sql.session.timeZone", testTimezone)
          val df = Seq(
            (
              valueOf("2020-01-15 08:01:32", testTimezone),
              valueOf("2020-01-14 21:00:00", testTimezone)
            ),
            (
              valueOf("2020-01-20 23:03:22", testTimezone),
              valueOf("2020-01-20 21:00:00", testTimezone)
            ),
            (null, null)
          ).toDF("some_time", "expected")
            .withColumn("actual", beginningOfDay(col("some_time"), "UTC"))
          assertColumnEquality(df, "actual", "expected")

          TimeZone.setDefault(defaultTimezone)
          spark.conf.set("spark.sql.session.timeZone", sparkDefaultTimezone)
        }
      }

      "beginningOfMonth" - {
        "gets the beginning of the month of a date column" - {
          val df = Seq(
            (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-01")),
            (Date.valueOf("2020-01-20"), Date.valueOf("2020-01-01")),
            (null, null)
          ).toDF("some_date", "expected")
          df.select("some_date").show()
          val resDF = df.withColumn("actual", beginningOfMonth(col("some_date")))
          resDF.select("some_date", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "gets the beginning of the month of a timestamp column" - {
          val df = Seq(
            (Timestamp.valueOf("2020-01-15 08:01:32"), Date.valueOf("2020-01-01")),
            (Timestamp.valueOf("2020-01-20 23:03:22"), Date.valueOf("2020-01-01")),
            (null, null)
          ).toDF("some_time", "expected")
            .withColumn("actual", beginningOfMonth(col("some_time")))
          assertColumnEquality(df, "actual", "expected")
        }
      }

      "endOfDay" - {
        "returns the end of the day" - {
          val df = Seq(
            (Timestamp.valueOf("2020-01-15 08:01:32"), Timestamp.valueOf("2020-01-15 23:59:59")),
            (Timestamp.valueOf("2020-01-20 23:03:22"), Timestamp.valueOf("2020-01-20 23:59:59")),
            (null, null)
          ).toDF("some_time", "expected")
          df.select("some_time").show()
          val resDF = df
            .withColumn("actual", endOfDay(col("some_time")))
          resDF.select("some_time", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "handles Dates correctly" - {
          val df = Seq(
            (Date.valueOf("2020-01-15"), Timestamp.valueOf("2020-01-15 23:59:59"))
          ).toDF("some_time", "expected")
          df.select("some_time").show()
          val resDF = df
            .withColumn("actual", endOfDay(col("some_time")))
          resDF.select("some_time", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "returns the end of the day in a specific timezone" - {
          println("timezone")
          val testTimezone         = "America/Bahia"
          val defaultTimezone      = TimeZone.getDefault()
          val sparkDefaultTimezone = spark.conf.get("spark.sql.session.timeZone")
          println(defaultTimezone.getDisplayName())
          println(sparkDefaultTimezone)
          TimeZone.setDefault(TimeZone.getTimeZone(testTimezone))
          spark.conf.set("spark.sql.session.timeZone", testTimezone)
          val df = Seq(
            (
              valueOf("2020-01-15 08:01:32", testTimezone),
              valueOf("2020-01-15 23:59:59", testTimezone)
            ),
            (
              valueOf("2020-01-20 23:03:22", testTimezone),
              valueOf("2020-01-20 23:59:59", testTimezone)
            ),
            (null, null)
          ).toDF("some_time", "expected")
            .withColumn("actual", endOfDay(col("some_time"), Some(testTimezone)))
          assertColumnEquality(df, "actual", "expected")

          TimeZone.setDefault(defaultTimezone)
          spark.conf.set("spark.sql.session.timeZone", sparkDefaultTimezone)
        }
      }

      "endOfMonth" - {
        "gets the end of the month of a date column" - {
          val df = Seq(
            (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-31")),
            (Date.valueOf("2018-02-02"), Date.valueOf("2018-02-28")),
            (null, null)
          ).toDF("some_date", "expected")
          df.select("some_date").show()
          val resDF = df.withColumn("actual", endOfMonth(col("some_date")))
          resDF.select("some_date", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "gets the end of the month of a timestamp column" - {
          val df = Seq(
            (Timestamp.valueOf("2020-01-15 08:01:32"), Date.valueOf("2020-01-31")),
            (Timestamp.valueOf("2020-02-02 23:03:22"), Date.valueOf("2020-02-29")),
            (null, null)
          ).toDF("some_time", "expected")
            .withColumn("actual", endOfMonth(col("some_time")))
          assertColumnEquality(df, "actual", "expected")
        }
      }

      // MISSING SPARK FUNCTIONS

      "bebe_approx_percentile" - {
        "calculates approximate percentiles" - {
          val df    = (1 to 1000).toDF("col")
          val resDF = df.select(bebe_approx_percentile(col("col"), array(lit(0.25), lit(0.99))))
          resDF.show()
        }
      }

      "bebe_cardinality" - {
        "returns the size of an array" - {
          val df = Seq(
            (Array("23", "44"), 2),
            (Array.empty[String], 0),
            (null, -1)
          ).toDF("some_strings", "expected")
          df.select("some_strings").show()
          val resDF = df.withColumn("actual", bebe_cardinality(col("some_strings")))
          resDF.select("some_strings", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "returns the size of a map" - {
          val df = Seq(
            (Map("23" -> 23, "44" -> 44), 2),
            (Map.empty[String, Int], 0),
            (null, -1)
          ).toDF("some_kv_pairs", "expected")
            .withColumn("actual", bebe_cardinality(col("some_kv_pairs")))
          assertColumnEquality(df, "actual", "expected")
        }
      }

      "bebe_cot" - {
        "returns the cotangent" - {
          val df = spark
            .createDF(
              List(
                (60, 3.12),
                (100, -1.7),
                (null, null)
              ),
              List(
                ("some_degree", IntegerType, true),
                ("expected", DoubleType, true)
              )
            )
          df.select("some_degree").show()
          val resDF = df.withColumn("actual", bebe_cot(col("some_degree")))
          resDF.select("some_degree", "actual").show()
          assertDoubleTypeColumnEquality(resDF, "actual", "expected", 0.01)
        }
      }

      "bebe_count_if" - {
        "returns the count if the predicate is true" - {
          val df = spark
            .createDF(
              List(
                (4),
                (3),
                (10)
              ),
              List(
                ("some_int", IntegerType, true)
              )
            )
          df.show()
          val resDF = df.agg(bebe_count_if(col("some_int") < 5).as("lt_five_count"))
          resDF.show()
          val expectedDF = spark
            .createDF(
              List(
                (2L)
              ),
              List(
                ("lt_five_count", LongType, true)
              )
            )
          assertSmallDataFrameEquality(resDF, expectedDF, ignoreNullable = true)
        }
      }

      "bebe_character_length" - {
        "returns the number of characters in a string" - {
          val df = spark
            .createDF(
              List(
                ("Spark SQL ", 10),
                ("", 0),
                (null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("expected", IntegerType, true)
              )
            )
          df.select("some_string").show()
          val resDF = df.withColumn("actual", bebe_character_length(col("some_string")))
          resDF.select("some_string", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "errors out when run on a column type that doesn't make sense" - {
          val df = spark
            .createDF(
              List(
                (33),
                (44),
                (null)
              ),
              List(
                ("some_int", IntegerType, true)
              )
            )
            .withColumn("actual", bebe_character_length(col("some_int")))
          intercept[org.apache.spark.sql.AnalysisException] {
            assertColumnEquality(df, "actual", "expected")
          }
        }
      }

      "bebe_chr" - {
        "returns the ASCII character of a character" - {
          val df = spark
            .createDF(
              List(
                (118, "v"),
                (65, "A"),
                (null, null)
              ),
              List(
                ("some_int", IntegerType, true),
                ("expected", StringType, true)
              )
            )
          df.select("some_int").show()
          val resDF = df.withColumn("actual", bebe_chr(col("some_int")))
          resDF.select("some_int", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_e" - {
        "returns Euler's number" - {
          val df = spark
            .createDF(
              List(
                (118, 2.718),
                (null, 2.718)
              ),
              List(
                ("some_int", IntegerType, true),
                ("expected", DoubleType, true)
              )
            )
          df.select("some_int").show()
          val resDF = df.withColumn("actual", bebe_e())
          resDF.select("some_int", "actual").show()
          assertDoubleTypeColumnEquality(resDF, "actual", "expected", 0.001)
        }
      }

      "bebe_if_null" - {
        "returns col2 if col1 isn't null" - {
          val df = spark
            .createDF(
              List(
                (null, "expr2", "expr2"),
                ("expr1", null, "expr1"),
                ("expr1", "expr2", "expr1")
              ),
              List(
                ("col1", StringType, true),
                ("col2", StringType, true),
                ("expected", StringType, true)
              )
            )
          df.select("col1", "col2").show()
          val resDF = df.withColumn("actual", bebe_if_null(col("col1"), col("col2")))
          resDF.select("col1", "col2", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_inline" - {
        "explodes an array of StructTypes to a table" - {
          val data = Seq(
            Row(20.0, "dog"),
            Row(3.5, "cat"),
            Row(0.000006, "ant")
          )

          val schema = StructType(
            List(
              StructField("weight", DoubleType, true),
              StructField("animal_type", StringType, true)
            )
          )

          val df = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
          )

          val actualDF = df
            .withColumn(
              "animal_interpretation",
              struct(
                (col("weight") > 5).as("is_large_animal"),
                col("animal_type").isin("rat", "cat", "dog").as("is_mammal")
              )
            )
            .groupBy("animal_interpretation")
            .agg(collect_list("animal_interpretation").as("interpretations"))

          actualDF.show()

          val res = actualDF.select(bebe_inline(col("interpretations")))
          res.show()

          val expected = spark.createDF(
            List(
              (true, true),
              (false, false),
              (false, true)
            ),
            List(
              ("is_large_animal", BooleanType, true),
              ("is_mammal", BooleanType, true)
            )
          )

          assertSmallDataFrameEquality(res, expected, orderedComparison = false)
        }
      }

      "bebe_is_not_null" - {
        "returns true if the element isn't null" - {
          val df = Seq(
            (null, false),
            ("hi", true)
          ).toDF("some_string", "expected")
          df.select("some_string").show()
          val resDF = df.withColumn("actual", bebe_is_not_null(col("some_string")))
          resDF.select("some_string", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_left" - {
        "gets the leftmost N elements from a string" - {
          val df = Seq(
            ("this 23 has 44 numbers", "th"),
            ("no numbers", "no"),
            (null, null)
          ).toDF("some_string", "expected")
          df.select("some_string").show(false)
          val resDF = df.withColumn("actual", bebe_left(col("some_string"), lit(2)))
          resDF.select("some_string", "actual").show(false)
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_like" - {
        "returns true if the pattern matches the SQL LIKE language" - {
          val df = spark
            .createDF(
              List(
                ("hi!", "hi_", true),
                ("hello there person", "hello%", true),
                ("whatever", "hello%", false),
                (null, null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("like_regexp", StringType, true),
                ("expected", BooleanType, true)
              )
            )
          df.select("some_string", "like_regexp").show()
          val resDF = df.withColumn("actual", bebe_like(col("some_string"), col("like_regexp")))
          resDF.select("some_string", "like_regexp", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_make_date" - {
        "creates a date" - {
          val df = spark
            .createDF(
              List(
                (2020, 1, 1, Date.valueOf("2020-01-01")),
                (2021, 3, 5, Date.valueOf("2021-03-05")),
                (null, null, null, null)
              ),
              List(
                ("year", IntegerType, true),
                ("month", IntegerType, true),
                ("day", IntegerType, true),
                ("expected", DateType, true)
              )
            )
          df.select("year", "month", "day").show()
          val resDF = df.withColumn("actual", bebe_make_date(col("year"), col("month"), col("day")))
          resDF.select("year", "month", "day", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_make_timestamp" - {
        "creates a date" - {
          val df = spark
            .createDF(
              List(
                (2020, 1, 1, 5, 3, 45, Timestamp.valueOf("2020-01-01 05:03:45")),
                (2021, 3, 5, 11, 1, 13, Timestamp.valueOf("2021-03-05 11:01:13")),
                (null, null, null, null, null, null, null)
              ),
              List(
                ("year", IntegerType, true),
                ("month", IntegerType, true),
                ("day", IntegerType, true),
                ("hours", IntegerType, true),
                ("minutes", IntegerType, true),
                ("seconds", IntegerType, true),
                ("expected", TimestampType, true)
              )
            )
          df.select("year", "month", "day", "hours", "minutes", "seconds").show()
          val resDF = df.withColumn(
            "actual",
            bebe_make_timestamp(
              col("year"),
              col("month"),
              col("day"),
              col("hours"),
              col("minutes"),
              col("seconds")
            )
          )
          resDF.select("year", "month", "day", "hours", "minutes", "seconds", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_nvl2" - {
        "Returns expr2 if expr1 is not null, or expr3 otherwise" - {
          val df = spark
            .createDF(
              List(
                (null, "expr2", "expr3", "expr3"),
                ("expr1", null, "expr3", null),
                ("expr1", "expr2", "expr3", "expr2"),
                ("expr1", null, null, null)
              ),
              List(
                ("col1", StringType, true),
                ("col2", StringType, true),
                ("col3", StringType, true),
                ("expected", StringType, true)
              )
            )
            .withColumn("actual", bebe_nvl2(col("col1"), col("col2"), col("col3")))
          assertColumnEquality(df, "actual", "expected")
        }
      }

      "bebe_octet_length" - {
        "calculates the octet length of a string" - {
          val df = spark
            .createDF(
              List(
                ("€", 3),
                ("Spark SQL", 9),
                (null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("expected", IntegerType, true)
              )
            )
          df.select("some_string").show()
          val resDF = df.withColumn("actual", bebe_octet_length(col("some_string")))
          resDF.select("some_string", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_parse_url" - {
        "extracts a part from the URL" - {
          val df = spark
            .createDF(
              List(
                ("http://spark.apache.org/path?query=1", "HOST", "spark.apache.org"),
                ("http://spark.apache.org/path?query=1", "QUERY", "query=1"),
                (null, null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("part_to_extract", StringType, true),
                ("expected", StringType, true)
              )
            )
          df.select("some_string", "part_to_extract").show(false)
          val resDF =
            df.withColumn("actual", bebe_parse_url(col("some_string"), col("part_to_extract")))
          resDF.select("some_string", "part_to_extract", "actual").show(false)
          assertColumnEquality(resDF, "actual", "expected")
        }

        "extracts a parameter value from a URL" - {
          val df = spark
            .createDF(
              List(
                ("http://spark.apache.org/path?funNumber=1", "QUERY", "funNumber", "1"),
                ("http://spark.apache.org/path", "QUERY", "funNumber", null),
                (null, null, null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("part_to_extract", StringType, true),
                ("urlParamKey", StringType, true),
                ("expected", StringType, true)
              )
            )
            .withColumn(
              "actual",
              bebe_parse_url(col("some_string"), col("part_to_extract"), col("urlParamKey"))
            )
          assertColumnEquality(df, "actual", "expected")
        }
      }

      "bebe_percentile" - {
        "computes the percentile" - {
          val df = spark
            .createDF(
              List(
                (0),
                (10)
              ),
              List(
                ("some_int", IntegerType, true)
              )
            )
          df.show()
          val resDF = df.agg(bebe_percentile(col("some_int"), lit(0.5)).as("50_percentile"))
          resDF.show()
          val expected = spark
            .createDF(
              List(
                (5.0)
              ),
              List(
                ("50_percentile", DoubleType, true)
              )
            )
          assertSmallDataFrameEquality(resDF, expected)
        }
      }

      "bebe_regexp_extract_all" - {
        "extracts multiple results" - {
          val df = Seq(
            ("this 23 has 44 numbers", Array("23", "44")),
            ("no numbers", Array.empty[String]),
            (null, null)
          ).toDF("some_string", "expected")
            .withColumn("actual", bebe_regexp_extract_all(col("some_string"), lit("(\\d+)"), lit(1)))
          assertColumnEquality(df, "actual", "expected")
        }
      }

      "bebe_right" - {
        "gets the rightmost N elements from a string" - {
          val df = Seq(
            ("this 23 has 44 numbers", "rs"),
            ("no dice", "ce"),
            (null, null)
          ).toDF("some_string", "expected")
          df.select("some_string").show(false)
          val resDF = df.withColumn("actual", bebe_right(col("some_string"), lit(2)))
          resDF.select("some_string", "actual").show(false)
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_sentences" - {
        "splits into an array of words" - {
          val df = Seq(
            ("Hi there! Good morning.", Array(Array("Hi", "there"), Array("Good", "morning"))),
            ("you are funny", Array(Array("you", "are", "funny"))),
            (null, null)
          ).toDF("some_string", "expected")
          df.select("some_string").show(false)
          val resDF = df.withColumn("actual", bebe_sentences(col("some_string")))
          resDF.select("some_string", "actual").show(false)
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_space" - {
        "creates spaces" - {
          val df = spark
            .createDF(
              List(
                ("some", "thing", 2, "some  thing"),
                ("like", "pizza", 3, "like   pizza"),
                (null, null, null, null)
              ),
              List(
                ("str1", StringType, true),
                ("str2", StringType, true),
                ("spaces", IntegerType, true),
                ("expected", StringType, true)
              )
            )
          df.select("str1", "str2", "spaces").show()
          val resDF =
            df.withColumn("actual", concat(col("str1"), bebe_space(col("spaces")), col("str2")))
          resDF.select("str1", "str2", "spaces", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }
      }

      "bebe_stack" - {
        "stacks stuff" - {
          val df = spark
            .createDF(
              List(
                (1, 2, 3, 4),
                (6, 7, 8, 9)
              ),
              List(
                ("col1", IntegerType, true),
                ("col2", IntegerType, true),
                ("col3", IntegerType, true),
                ("col4", IntegerType, true)
              )
            )
          df.show()
          val resDF = df.select(bebe_stack(lit(2), col("col1"), col("col2"), col("col3"), col("col4")))
          resDF.show()
          val expectedDF = spark.createDF(
            List(
              (1, 2),
              (3, 4),
              (6, 7),
              (8, 9)
            ),
            List(
              ("col0", IntegerType, true),
              ("col1", IntegerType, true)
            )
          )
          assertSmallDataFrameEquality(resDF, expectedDF)
        }
      }

      "bebe_substr" - {
        "gets tail elements, starting at pos.  start counting from right if pos is negative" - {
          val df = spark
            .createDF(
              List(
                // start counting from left with 1 indexing
                ("brasil", 3, "asil"),
                // can also start counting from the right
                ("peru", -2, "ru"),
                (null, null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("pos", IntegerType, true),
                ("expected", StringType, true)
              )
            )
          df.select("some_string", "pos").show()
          val resDF = df.withColumn("actual", bebe_substr(col("some_string"), col("pos")))
          resDF.select("some_string", "pos", "actual").show()
          assertColumnEquality(resDF, "actual", "expected")
        }

        "gets len elements, starting at pos" - {
          val df = spark
            .createDF(
              List(
                ("aaabbbccc", 3, 4, "abbb"),
                ("aaabbbccc", 1, 3, "aaa"),
                (null, null, null, null)
              ),
              List(
                ("some_string", StringType, true),
                ("pos", IntegerType, true),
                ("len", IntegerType, true),
                ("expected", StringType, true)
              )
            )
            .withColumn("actual", bebe_substr(col("some_string"), col("pos"), col("len")))
          assertColumnEquality(df, "actual", "expected")
        }
      }

      "bebe_uuid" - {
        "returns a uuid" - {
          val df = Seq(
            ("hi", 36),
            ("bye", 36)
          ).toDF("some_string", "expected_length")
          df.withColumn("actual", bebe_uuid()).select("actual").show(false)
          val resDF = df
            .withColumn("actual", bebe_uuid())
            .withColumn("actual_length", length(col("actual")))
          assertColumnEquality(resDF, "actual_length", "expected_length")
        }
      }

      "bebe_weekday" - {
        "returns an integer corresponding to the weekday" - {
          val df = spark
            .createDF(
              List(
                (Date.valueOf("2021-03-15"), 0),
                (Date.valueOf("2021-03-17"), 2),
                (Date.valueOf("2021-03-21"), 6),
                (null, null)
              ),
              List(
                ("some_date", DateType, true),
                ("expected", IntegerType, true)
              )
            )
          df.select("some_date").show(false)
          val resDF = df.withColumn("actual", bebe_weekday(col("some_date")))
          resDF.select("some_date", "actual").show(false)
          assertColumnEquality(resDF, "actual", "expected")
        }
      }
    }
}
