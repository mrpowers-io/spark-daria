package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import org.apache.spark.sql.types.{StringType, IntegerType}
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

}
