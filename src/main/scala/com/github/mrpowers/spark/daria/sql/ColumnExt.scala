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

    def nullBetween(lowerCol: Column, upperCol: Column): Column = {
      when(lowerCol.isNull && upperCol.isNull, false).otherwise(
        when(col.isNull, false).otherwise(
          when(lowerCol.isNull && upperCol.isNotNull && col.leq(upperCol), true).otherwise(
            when(lowerCol.isNotNull && upperCol.isNull && col.geq(lowerCol), true).otherwise(
              col.between(lowerCol, upperCol)
            )
          )
        )
      )
    }

    def isFalsy: Column = {
      when(col.isNull || col === lit(false), true).otherwise(false)
    }

    def isTruthy: Column = {
      !col.isFalsy
    }

    def isNullOrBlank: Column = {
      col.isNull || trim(col) === ""
    }

    def isNotNullOrBlank: Column = {
      !col.isNullOrBlank
    }

    def isNotIn(list: Any*): Column = {
      not(col.isin(list: _*))
    }

  }

}
