package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object BebeRight {
  def apply(col: Column, len: Column): Right = {
    Right(
      col.expr,
      len.expr,
      If(
        IsNull(col.expr),
        Literal(null, StringType),
        If(
          LessThanOrEqual(len.expr, Literal(0)),
          Literal(UTF8String.EMPTY_UTF8, StringType),
          new Substring(col.expr, UnaryMinus(len.expr))
        )
      )
    )
  }
}