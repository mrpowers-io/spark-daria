package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column

object FunctionsAsColumnExt {

  implicit class ColumnMethods(col: Column) {

    def initcap(): Column = {
      org.apache.spark.sql.functions.initcap(col)
    }

    def length(): Column = {
      org.apache.spark.sql.functions.length(col)
    }

    def lower(): Column = {
      org.apache.spark.sql.functions.lower(col)
    }

    def regexp_replace(pattern: String, replacement: String): Column = {
      org.apache.spark.sql.functions.regexp_replace(
        col,
        pattern,
        replacement
      )
    }

  }

}
