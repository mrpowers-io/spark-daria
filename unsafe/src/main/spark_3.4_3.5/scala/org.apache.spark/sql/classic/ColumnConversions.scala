package org.apache.spark.sql.classic

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object ColumnConversions {
  def expression(column: Column): Expression = column.expr
}
