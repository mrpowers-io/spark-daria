package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class TransformationsSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  describe("#snakeCaseColumns") {

    it("snake_cases the columns of a DataFrame") {

      val sourceDF = Seq(
        ("funny", "joke")
      ).toDF("A b C", "de F")

      val actualDF = sourceDF.transform(transformations.snakeCaseColumns)

      val expectedDF = Seq(
        ("funny", "joke")
      ).toDF("a_b_c", "de_f")

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
