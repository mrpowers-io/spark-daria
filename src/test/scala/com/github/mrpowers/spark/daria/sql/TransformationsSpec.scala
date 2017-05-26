package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import org.apache.spark.sql.types.StringType
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class TransformationsSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

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
