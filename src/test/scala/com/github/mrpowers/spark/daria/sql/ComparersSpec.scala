package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.sql.functions._
import SparkSessionExt._

class ComparersSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

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
        Comparers.nullBetween(col("age"), col("lower_age"), col("upper_age"))
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
      Comparers.nullBetween(
        playersDF("age"),
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

}
