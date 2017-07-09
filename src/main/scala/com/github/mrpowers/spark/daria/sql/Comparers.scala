package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Comparers {

  def nullBetween(value: Column, lowerCol: Column, upperCol: Column): Column = {
    when(lowerCol.isNull && upperCol.isNull, false).otherwise(
      when(value.isNull, false).otherwise(
        when(lowerCol.isNull && upperCol.isNotNull && value.leq(upperCol), true).otherwise(
          when(lowerCol.isNotNull && upperCol.isNull && value.geq(lowerCol), true).otherwise(
            value.between(lowerCol, upperCol)
          )
        )
      )
    )
  }

}
