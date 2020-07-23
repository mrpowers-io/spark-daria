package com.github.mrpowers.spark.daria.elt

import utest._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import com.github.mrpowers.spark.daria.sql.SparkSessionTestWrapper

object StagingParserTest extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {
  val tests = Tests {

    'parse - {
      "parses a DataFrame" - {
        val path = new java.io.File("./src/test/resources/zipcodes.txt").getCanonicalPath
        val sourceDF = spark.read
          .text(path)
          .select(struct("*") as 'origin)
        val schema = StructType(
          Seq(
            StructField("zip", StringType, true),
            StructField("city", StringType, true),
            StructField("state", StringType, true),
            StructField("ts", StringType, true)
          )
        )
        val sp       = new StagingParser(schema)
        val actualDF = sp.parse(sourceDF).drop("origin")
        val expectedDF = spark.createDF(
          List(
            ("10506", "Bedford", "NY", "111"),
            ("95053", "Santa Clara", "CA", "222"),
            ("86001", "Flagstaff", "AZ", "333"),
            (null, null, null, "666")
          ),
          List(
            ("zip", StringType, true),
            ("city", StringType, true),
            ("state", StringType, true),
            ("ts", StringType, true)
          )
        )
        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'setParseDetails - {
      "parses a DataFrame" - {
        val path = new java.io.File("./src/test/resources/zipcodes.txt").getCanonicalPath
        val sourceDF = spark.read
          .text(path)
          .select(struct("*") as 'origin)
        val schema = StructType(
          Seq(
            StructField("zip", StringType, true),
            StructField("city", StringType, true),
            StructField("state", StringType, true),
            StructField("ts", StringType, true)
          )
        )
        val sp       = new StagingParser(schema)
        val parsedDF = sp.parse(sourceDF)
        val actualDF = sp
          .setParseDetails(parsedDF)
          .flattenSchema("-")
          .select("zip", "parse_details-status")
        val expectedDF = spark.createDF(
          List(
            ("10506", "OK"),
            ("95053", "OK"),
            ("86001", "OK"),
            (null, "NOT_VALID")
          ),
          List(
            ("zip", StringType, true),
            ("parse_details-status", StringType, false)
          )
        )
        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

    'apply - {
      "parses a DataFrame" - {
        val path     = new java.io.File("./src/test/resources/zipcodes.txt").getCanonicalPath
        val sourceDF = spark.read.text(path)
        val schema = StructType(
          Seq(
            StructField("zip", StringType, true),
            StructField("city", StringType, true),
            StructField("state", StringType, true),
            StructField("ts", StringType, true)
          )
        )
        val sp       = new StagingParser(schema)
        val actualDF = sp.apply(sourceDF).flattenSchema("-").select("ts", "parse_details-status")
        val expectedDF = spark.createDF(
          List(
            ("111", "OK"),
            ("222", "OK"),
            ("333", "OK"),
            ("666", "NOT_VALID")
          ),
          List(
            ("ts", StringType, true),
            ("parse_details-status", StringType, false)
          )
        )
        assertSmallDataFrameEquality(
          actualDF,
          expectedDF
        )
      }
    }

  }
}
