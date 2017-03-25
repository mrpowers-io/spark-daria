package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

package object functions {

  def yeardiff(end: Column, start: Column): Column = {
    datediff(end, start) / 365
  }

  def between(col: Column, min: Any, max: Any): Column = {
    col.geq(min) && col.leq(max)
  }

  def rpadDaria(len: Integer, pad: String)(col: Column): Column = {
    rpad(col, len, pad)
  }

}
