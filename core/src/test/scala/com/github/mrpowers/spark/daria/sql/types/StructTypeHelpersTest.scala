package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.types._
import utest._

object StructTypeHelpersTest extends TestSuite {

  val tests = Tests {

    'build - {
      "creates a StructType" - {
        val actualSchema = StructTypeHelpers.build(
          ("some_col", StringType, true),
          ("another_col", IntegerType, false)
        )
        val expectedSchema = StructType(
          List(
            StructField("some_col", StringType, true),
            StructField("another_col", IntegerType, false)
          )
        )
        assert(actualSchema == expectedSchema)
      }
    }

    'flattenSchema - {
      "converts all the StructTypes to regular columns" - {
        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true)
          )
        )
        StructTypeHelpers.flattenSchema(schema)
      }

      "converts nested StructType schemas" - {
        val schema = StructType(
          Seq(
            StructField("a", StringType, true),
            StructField("b", StringType, true),
            StructField(
              "c",
              StructType(Seq(StructField("foo", StringType, true), StructField("bar", StringType, true))),
              true
            )
          )
        )
        StructTypeHelpers.flattenSchema(schema)
      }
    }

    'schemaFor - {
      "gets schema from a scala Type" - {
        val actualSchema = StructTypeHelpers.schemaFor[FooBar]
        val expectedSchema = StructType(
          List(
            StructField("foo", IntegerType, false),
            StructField("bar", StringType),
            StructField("foo_bar", ArrayType(IntegerType, false))
          )
        )
        assert(actualSchema == expectedSchema)
      }
    }

  }
  // this needs to be outside of the tests block to compile correctly
  case class FooBar(foo: Int, bar: String, fooBar: Array[Int])

}
