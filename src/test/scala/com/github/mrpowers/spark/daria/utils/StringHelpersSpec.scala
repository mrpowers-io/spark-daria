package com.github.mrpowers.spark.daria.utils

import org.scalatest.FunSpec

class StringHelpersSpec extends FunSpec {

  describe("escapeForSqlRegexp") {

    it("escapes all the special characters in a string") {
      assert(StringHelpers.escapeForSqlRegexp("D/E") === Some("D\\/E"))
      assert(StringHelpers.escapeForSqlRegexp("(E/F)") === Some("\\(E\\/F\\)"))
      assert(StringHelpers.escapeForSqlRegexp("") === Some(""))
      assert(StringHelpers.escapeForSqlRegexp("E|G") === Some("E\\|G"))
      assert(StringHelpers.escapeForSqlRegexp("E;;G") === Some("E;;G"))
      assert(StringHelpers.escapeForSqlRegexp("^AB-C") === Some("^AB\\-C"))
      assert(StringHelpers.escapeForSqlRegexp(null) === None)
    }

  }

}
