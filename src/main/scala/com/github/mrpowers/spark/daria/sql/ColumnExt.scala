package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column

object ColumnExt {

  implicit class ColumnMethods(c: Column) {

    def chain(t: (Column => Column)): Column = {
      t(c)
    }

  }

}
