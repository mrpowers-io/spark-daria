package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.Column

object BebeRight {
  def apply(str: Column, len: Column): Right = {
    Right(str.expr, len.expr)
  }
}