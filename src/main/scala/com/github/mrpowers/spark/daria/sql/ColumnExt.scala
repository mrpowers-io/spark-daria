package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnExt {

  implicit class ColumnMethods(col: Column) {

    def chain(colMethod: (Column => Column)): Column = {
      colMethod(col)
    }

    def chainUDF(udfName: String, cols: Column*): Column = {
      callUDF(udfName, col +: cols: _*)
    }

  }

}
