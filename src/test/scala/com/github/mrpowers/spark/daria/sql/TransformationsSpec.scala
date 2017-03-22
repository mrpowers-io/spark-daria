package com.github.mrpowers.spark.daria.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

class TransformationsSpec extends FunSpec with DataFrameSuiteBase {

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

      assertDataFrameEquals(actualDF, expectedDF)

    }

  }

}
