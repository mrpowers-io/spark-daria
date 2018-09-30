package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column

object FunctionsAsColumnExt {

  implicit class ColumnMethods(col: Column) {

    def lower(): Column = {
      org.apache.spark.sql.functions.lower(col)
    }

  }

}
