package com.github.mrpowers.spark.daria.utils

import utest._

object StringHelpersTest extends TestSuite {

  val tests = Tests {

    'escapeForSqlRegexp - {
      assert(StringHelpers.escapeForSqlRegexp("D/E") == Some("D\\/E"))
      assert(StringHelpers.escapeForSqlRegexp("(E/F)") == Some("\\(E\\/F\\)"))
      assert(StringHelpers.escapeForSqlRegexp("") == Some(""))
      assert(StringHelpers.escapeForSqlRegexp("E|G") == Some("E\\|G"))
      assert(StringHelpers.escapeForSqlRegexp("E;;G") == Some("E;;G"))
      assert(StringHelpers.escapeForSqlRegexp("^AB-C") == Some("^AB\\-C"))
      assert(StringHelpers.escapeForSqlRegexp("^AB+C") == Some("^AB\\+C"))
      assert(StringHelpers.escapeForSqlRegexp(null) == None)
    }

    'toSnakeCase - {
      assert(StringHelpers.toSnakeCase("A b C") == "a_b_c")
    }

    'snakify - {
      assert(StringHelpers.snakify("SomeColumn") == "some_column")
    }

    'camelCaseToSnakeCase - {
      assert(StringHelpers.camelCaseToSnakeCase("thisIsCool") == "this_is_cool")
    }

    'parquetCompatibleColumnName - {
      assert(StringHelpers.parquetCompatibleColumnName("Column That {Will} Break\t;") == "column_that_will_break")
    }

  }

}
