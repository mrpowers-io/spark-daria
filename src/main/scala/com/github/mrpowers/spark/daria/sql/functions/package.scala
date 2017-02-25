package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

package object functions {

  def yeardiff(end: Column, start: Column): Column = {
    datediff(end, start) / 365
  }

  def between(c: Column, min: Any, max: Any): Column = {
    c.geq(min) && c.leq(max)
  }

  def rpadDaria(len: Integer, pad: String)(c: Column): Column = {
    rpad(c, len, pad)
  }

}
