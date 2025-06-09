package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.DataType

case class KnownNotNull(child: Expression) extends TaggingExpression {
  override def dataType: DataType = child.dataType

  override def nullable: Boolean = false

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx).copy(isNull = FalseLiteral)
  }

  override protected def withNewChildInternal(newChild: Expression): KnownNotNull =
    copy(child = newChild)

  override def eval(input: InternalRow): Any = child.eval(input)
}