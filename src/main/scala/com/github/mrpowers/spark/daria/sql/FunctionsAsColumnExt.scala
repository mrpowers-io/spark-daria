package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column

object FunctionsAsColumnExt {

  implicit class ColumnMethods(col: Column) {
    def |(f: Column => Column): Column = f(col)
  }

}
