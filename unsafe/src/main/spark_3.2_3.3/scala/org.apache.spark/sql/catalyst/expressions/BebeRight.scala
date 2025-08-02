package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class BebeRight(str: Expression, len: Expression) extends RuntimeReplaceable
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  override lazy val replacement: Expression = If(
    IsNull(str),
    Literal(null, StringType),
    If(
      LessThanOrEqual(len, Literal(0)),
      Literal(UTF8String.EMPTY_UTF8, StringType),
      new Substring(str, UnaryMinus(len))
    )
  )

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType)
  override def left: Expression = str
  override def right: Expression = len
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, len = newRight)
  }
}
