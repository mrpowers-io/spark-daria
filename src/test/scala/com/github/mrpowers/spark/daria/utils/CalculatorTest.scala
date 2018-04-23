package com.github.mrpowers.spark.daria.utils

import utest._

object CalculatorTest extends TestSuite {

  val tests = Tests {

    'add - {

      "adds two integers together" - {
        assert(Calculator.add(2, 3) == 5)
      }

    }

  }

}
