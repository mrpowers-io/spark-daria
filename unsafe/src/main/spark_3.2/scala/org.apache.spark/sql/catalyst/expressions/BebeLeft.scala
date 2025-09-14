package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.Column

object BebeLeft {
  def apply(str: Column, len: Column): Left = {
    Left(str.expr, len.expr, Substring(str.expr, Literal(1), len.expr))
  }
}