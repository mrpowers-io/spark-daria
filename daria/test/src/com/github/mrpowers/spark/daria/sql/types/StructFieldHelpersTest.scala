package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.types._
import utest._

object StructFieldHelpersTest extends TestSuite {

  val tests = Tests {

    'customEquals - {

      "returns true if two StructFields have the same name, datatype, and nullable property" - {
        val s1 = StructField(
          "some_col",
          StringType,
          true
        )
        val s2 = StructField(
          "some_col",
          StringType,
          true
        )
        assert(
          StructFieldHelpers.customEquals(
            s1,
            s2
          )
        )
      }

      "returns false if StructFields have different names" - {
        val s1 = StructField(
          "some_col",
          StringType,
          true
        )
        val s2 = StructField(
          "col2",
          StringType,
          true
        )
        assert(
          StructFieldHelpers.customEquals(
            s1,
            s2
          ) == false
        )
      }

      "returns false if StructFields have different data types" - {
        val s1 = StructField(
          "some_col",
          StringType,
          true
        )
        val s2 = StructField(
          "some_col",
          IntegerType,
          true
        )
        assert(
          StructFieldHelpers.customEquals(
            s1,
            s2
          ) == false
        )
      }

      "returns false if StructFields have different nullable properties" - {
        val s1 = StructField(
          "some_col",
          StringType,
          true
        )
        val s2 = StructField(
          "some_col",
          StringType,
          false
        )
        assert(
          StructFieldHelpers.customEquals(
            s1,
            s2
          ) == false
        )
      }

      "ignores nullable differences if the flag is set" - {
        val s1 = StructField(
          "some_col",
          StringType,
          true
        )
        val s2 = StructField(
          "some_col",
          StringType,
          false
        )
        assert(
          StructFieldHelpers.customEquals(
            s1,
            s2,
            true
          ) == true
        )
      }

      "also returns true when ignoreNullable is true but the nullable property is the same" - {
        val s1 = StructField(
          "some_col",
          StringType,
          true
        )
        val s2 = StructField(
          "some_col",
          StringType,
          true
        )
        assert(
          StructFieldHelpers.customEquals(
            s1,
            s2,
            true
          ) == true
        )
      }

    }

  }

}
