package com.github.mrpowers.spark.daria.ml

import com.github.mrpowers.spark.daria.sql.SparkSessionTestWrapper
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import utest._
import com.github.mrpowers.spark.fast.tests.ColumnComparer

object TransformationsTest
    extends TestSuite
    with ColumnComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'withVectorizedFeatures - {

      "converts all the features to a vector without blowing up" - {

        val df = spark.createDF(
          List(
            (1.0, 12.0, org.apache.spark.mllib.linalg.Vectors.dense(1.0, 12.0))), List(
            ("gender", DoubleType, true),
            ("age", DoubleType, true),
            ("expected", new org.apache.spark.mllib.linalg.VectorUDT, true))).transform(transformations.withVectorizedFeatures(Array("gender", "age")))

      }

    }

    'withLabel - {

      "adds a label column" - {

        val df = spark.createDF(
          List(
            (0.0, 1.0),
            (1.0, 0.0)), List(
            ("survived", DoubleType, true),
            ("expected", DoubleType, true)))
          .transform(transformations.withLabel("survived"))

        assertColumnEquality(df, "expected", "label")

      }

      "works if the label column is a string" - {

        val df = spark.createDF(
          List(
            ("no", 0.0),
            ("yes", 1.0),
            ("hi", 2.0),
            ("no", 0.0)), List(
            ("survived", StringType, true),
            ("expected", DoubleType, true)))
          .transform(transformations.withLabel("survived"))

        assertColumnEquality(df, "expected", "label")

      }

    }

  }

}
