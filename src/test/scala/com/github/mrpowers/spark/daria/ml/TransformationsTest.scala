package com.github.mrpowers.spark.daria.ml

import com.github.mrpowers.spark.daria.sql.SparkSessionTestWrapper
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.DoubleType
import utest._

object TransformationsTest
    extends TestSuite
    with SparkSessionTestWrapper {

  val tests = Tests {

    'withVectorizedFeatures - {

      "converts all the features to a vector without blowing up" - {

        val df = spark.createDF(
          List(
            (1.0, 12.0, org.apache.spark.mllib.linalg.Vectors.dense(1.0, 12.0))
          ), List(
            ("gender", DoubleType, true),
            ("age", DoubleType, true),
            ("expected", new org.apache.spark.mllib.linalg.VectorUDT, true)
          )
        ).transform(transformations.withVectorizedFeatures(Array("gender", "age")))

      }

    }

  }

}
