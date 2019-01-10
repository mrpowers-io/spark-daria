package com.github.mrpowers.spark.daria.utils

import utest._

class StringHelpersSpec extends TestSuite {

  val tests = Tests {

    'escapeForSqlRegexp - {

      "escapes all the special characters in a string" - {
        assert(StringHelpers.escapeForSqlRegexp("D/E") == Some("D\\/E"))
        assert(StringHelpers.escapeForSqlRegexp("(E/F)") == Some("\\(E\\/F\\)"))
        assert(StringHelpers.escapeForSqlRegexp("") == Some(""))
        assert(StringHelpers.escapeForSqlRegexp("E|G") == Some("E\\|G"))
        assert(StringHelpers.escapeForSqlRegexp("E;;G") == Some("E;;G"))
        assert(StringHelpers.escapeForSqlRegexp("^AB-C") == Some("^AB\\-C"))
        assert(StringHelpers.escapeForSqlRegexp("^AB+C") == Some("^AB\\+C"))
        assert(StringHelpers.escapeForSqlRegexp(null) == None)
      }

    }

  }

}
