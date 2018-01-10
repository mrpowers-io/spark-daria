package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnExt {

  implicit class ColumnMethods(col: Column) {

    /** Chains column functions */
    def chain(colMethod: (Column => Column)): Column = {
      colMethod(col)
    }

    /** Chains UDFs */
    def chainUDF(udfName: String, cols: Column*): Column = {
      callUDF(udfName, col +: cols: _*)
    }

    /** Like between, but geq when upper bound is null and leq when lower bound is null */
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

    /** Returns true if the current expression is true */
    def isTrue: Column = {
      when(col.isNull, false).otherwise(col === true)
    }

    /** Returns true if the col is false */
    def isFalse: Column = {
      when(col.isNull, false).otherwise(col === false)
    }

    /** Returns true if the col is false or null */
    def isFalsy: Column = {
      when(col.isNull || col === lit(false), true).otherwise(false)
    }

    /** Returns true if the col is not false or null */
    def isTruthy: Column = {
      !col.isFalsy
    }

    /** Returns true if the col is null or a blank string */
    def isNullOrBlank: Column = {
      col.isNull || trim(col) === ""
    }

    /** Returns true if the col is not null or a blank string */
    def isNotNullOrBlank: Column = {
      !col.isNullOrBlank
    }

    /** Returns true if the col is not in a list of elements */
    def isNotIn(list: Any*): Column = {
      not(col.isin(list: _*))
    }

  }

}
