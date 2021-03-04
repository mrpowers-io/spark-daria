package com.github.mrpowers.spark.daria.utils

import org.apache.spark.sql.Row
import utest._

object RowHelpersTest extends TestSuite {

  val tests = Tests {

    "prettyPrintRow" - {
      val r = Row("hi", 3.4, null, 5L)
      RowHelpers.prettyPrintRow(r)
    }

  }

}
