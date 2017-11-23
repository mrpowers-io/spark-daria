package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType}

import org.scalatest.FunSpec

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class TransformationsSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  describe("#sortColumns") {

    it("sorts the columns in a DataFrame in ascending order") {

      val sourceDF = spark.createDF(
        List(
          ("pablo", 3, "polo")
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true),
          ("sport", StringType, true)
        )
      )

      val actualDF = sourceDF.transform(transformations.sortColumns())

      val expectedDF = spark.createDF(
        List(
          (3, "pablo", "polo")
        ), List(
          ("age", IntegerType, true),
          ("name", StringType, true),
          ("sport", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("sorts the columns in a DataFrame in descending order") {

      val sourceDF = spark.createDF(
        List(
          ("pablo", 3, "polo")
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true),
          ("sport", StringType, true)
        )
      )

      val actualDF = sourceDF.transform(transformations.sortColumns("desc"))

      val expectedDF = spark.createDF(
        List(
          ("polo", "pablo", 3)
        ), List(
          ("sport", StringType, true),
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("throws an error if the sort order is invalid") {

      val sourceDF = spark.createDF(
        List(
          ("pablo", 3, "polo")
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true),
          ("sport", StringType, true)
        )
      )

      intercept[InvalidColumnSortOrderException] {
        sourceDF.transform(transformations.sortColumns("cats"))
      }

    }

  }

  describe("#snakeCaseColumns") {

    it("snake_cases the columns of a DataFrame") {

      val sourceDF = spark.createDF(
        List(
          ("funny", "joke")
        ), List(
          ("A b C", StringType, true),
          ("de F", StringType, true)
        )
      )

      val actualDF = sourceDF.transform(transformations.snakeCaseColumns)

      val expectedDF = spark.createDF(
        List(
          ("funny", "joke")
        ), List(
          ("a_b_c", StringType, true),
          ("de_f", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#multiRegexpReplace") {

    it("remove characters that match a regular expression in the columns of a DataFrame") {

      val sourceDF = spark.createDF(
        List(
          ("\u0000StringTest", 123)
        ), List(
          ("StringTypeCol", StringType, true),
          ("IntegerTypeCol", IntegerType, true)
        )
      )

      val actualDF = sourceDF.transform(
        transformations.multiRegexpReplace(
          List(
            col("StringTypeCol")
          ),
          "\u0000",
          "ThisIsA"
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("ThisIsAStringTest", 123)
        ), List(
          ("StringTypeCol", StringType, true),
          ("IntegerTypeCol", IntegerType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("removes \\x00 in the columns of a DataFrame") {

      val sourceDF = spark.createDF(
        List(
          ("\\x00", 123)
        ), List(
          ("StringTypeCol", StringType, true),
          ("num", IntegerType, true)
        )
      )

      val actualDF = sourceDF.transform(
        transformations.multiRegexpReplace(
          List(col("StringTypeCol"), col("num")),
          "\\\\x00",
          ""
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("", "123")
        ), List(
          ("StringTypeCol", StringType, true),
          ("num", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

    it("works on multiple columns") {

      val sourceDF = spark.createDF(
        List(
          ("Bart cool", "moto cool"),
          ("cool James", "droid fun")
        ), List(
          ("person", StringType, true),
          ("phone", StringType, true)
        )
      )

      val actualDF = sourceDF.transform(
        transformations.multiRegexpReplace(
          List(col("person"), col("phone")),
          "cool",
          "dude"
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("Bart dude", "moto dude"),
          ("dude James", "droid fun")
        ), List(
          ("person", StringType, true),
          ("phone", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#bulkRegexpReplace") {

    it("works on all the StringType columns") {

      val sourceDF = spark.createDF(
        List(
          ("Bart cool", "moto cool", 5),
          ("cool James", "droid fun", 10)
        ), List(
          ("person", StringType, true),
          ("phone", StringType, true),
          ("num", IntegerType, true)
        )
      )

      val actualDF = sourceDF.transform(
        transformations.bulkRegexpReplace(
          "cool",
          "dude"
        )
      )

      val expectedDF = spark.createDF(
        List(
          ("Bart dude", "moto dude", 5),
          ("dude James", "droid fun", 10)
        ), List(
          ("person", StringType, true),
          ("phone", StringType, true),
          ("num", IntegerType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#truncateColumns") {

    it("truncates columns based on specified lengths") {

      val sourceDF = spark.createDF(
        List(
          ("Bart cool", "moto cool"),
          ("cool James", "droid fun"),
          (null, null)
        ), List(
          ("person", StringType, true),
          ("phone", StringType, true)
        )
      )

      val columnLengths: Map[String, Int] = Map(
        "person" -> 2,
        "phone" -> 3,
        "whatever" -> 50000
      )

      val actualDF = sourceDF.transform(
        transformations.truncateColumns(columnLengths)
      )

      val expectedDF = spark.createDF(
        List(
          ("Ba", "mot"),
          ("co", "dro"),
          (null, null)
        ), List(
          ("person", StringType, true),
          ("phone", StringType, true)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
