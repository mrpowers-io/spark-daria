package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, IntegerType, StringType, TypeCollection}

case class BebeLeft(str: Expression, len: Expression) extends RuntimeReplaceable
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  override lazy val replacement: Expression = Substring(str, Literal(1), len)

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(StringType, BinaryType), IntegerType)
  }

  override def left: Expression = str
  override def right: Expression = len
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, len = newRight)
  }
}
