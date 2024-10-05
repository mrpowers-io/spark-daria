package com.github.mrpowers.spark.daria.utils

import utest._

object ArrayHelpersTest extends TestSuite {

  val tests = Tests {

    'regexpString - {
      "converts an array of strings to an escaped regexp string" - {
        val origArray = Array(
          "D/E",
          "(E/F)",
          "",
          "E|G",
          "E;;G",
          "^AB-C",
          null
        )
        val actualStr   = ArrayHelpers.regexpString(origArray)
        val expectedStr = """D\/E|\(E\/F\)||E\|G|E;;G|^AB\-C"""
        assert(actualStr == expectedStr)
      }
    }

  }

}
