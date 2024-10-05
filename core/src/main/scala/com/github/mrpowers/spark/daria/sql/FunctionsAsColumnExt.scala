package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => F}

object FunctionsAsColumnExt {

  implicit class ColumnMethods(col: Column) {
    private def t(f: Column => Column): Column = f(col)

    def initcap(): Column = t(F.initcap)

    def length(): Column = t(F.length)

    def lower(): Column = t(F.lower)

    def regexp_replace(pattern: String, replacement: String): Column =
      F.regexp_replace(col, pattern, replacement)

  }

}
