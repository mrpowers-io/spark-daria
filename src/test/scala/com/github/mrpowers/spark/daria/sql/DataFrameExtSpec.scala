package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}
import DataFrameExt._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame

class DataFrameExtSpec
    extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  describe("#printSchemaInCodeFormat") {

    it("prints the schema in a code friendly format") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football", 45),
          ("nacional", "soccer", 10)
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true),
          ("goals_for", IntegerType, true)
        )
      )

      //      uncomment the next line if you want to check out the console output
      //      sourceDF.printSchemaInCodeFormat()

    }

  }

  describe("#composeTransforms") {

    it("runs a list of transforms") {

      val sourceDF = spark.createDF(
        List(
          ("jets"),
          ("nacional")
        ), List(
          ("team", StringType, true)
        )
      )

      val transforms = List(
        ExampleTransforms.withGreeting()(_),
        ExampleTransforms.withCat("sandy")(_)
      )

      val actualDF = sourceDF.composeTransforms(transforms)

      val expectedDF = spark.createDF(
        List(
          ("jets", "hello world", "sandy meow"),
          ("nacional", "hello world", "sandy meow")
        ), List(
          ("team", StringType, true),
          ("greeting", StringType, false),
          ("cats", StringType, false)
        )
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

  describe("#containsColumn") {

    it("returns true if a DataFrame contains a column") {

      val sourceDF = spark.createDF(
        List(
          ("jets"),
          ("nacional")
        ), List(
          ("team", StringType, true)
        )
      )

      assert(sourceDF.containsColumn("team") === true)
      assert(sourceDF.containsColumn("blah") === false)

    }

  }

}
