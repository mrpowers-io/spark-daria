package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnExt {

  implicit class ColumnMethods(c: Column) {

    def chain(t: (Column => Column)): Column = {
      t(c)
    }

    def chainUDF(udfName: String): Column = {
      callUDF(udfName, c)
    }

  }

}
