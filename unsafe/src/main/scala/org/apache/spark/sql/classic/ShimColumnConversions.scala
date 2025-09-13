package org.apache.spark.sql.classic

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Shim layer for ColumnConversion in preparation for spark 4.0 which removed Column.expr
 * https://github.com/apache/spark/blob/d427aa3c6f473e33c60eb9f8c7211dfb76cc3e3d/sql/core/src/main/scala/org/apache/spark/sql/classic/conversions.scala
 */
object ShimColumnConversions {
  def columnToExpression(column: Column): Expression = {
    ColumnConversions.expression(column)
  }

  implicit class ShimColumnConversionsOps(column: Column) {
    def toExpression: Expression = columnToExpression(column)
  }
}
