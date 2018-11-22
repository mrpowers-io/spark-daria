package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.types._
import utest._

object StructTypeHelpersTest extends TestSuite {

  val tests = Tests {

    'flattenSchema - {

      "converts all the StructTypes to regular columns" - {

        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true)))

        StructTypeHelpers.flattenSchema(schema)

      }

      "converts nested StructType schemas" - {

        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true),
            StructField(
              "c",
              StructType(
                Seq(
                  StructField("foo", StringType, true),
                  StructField("bar", StringType, true))),
              true)))

        StructTypeHelpers.flattenSchema(schema)

      }

    }

  }

}
