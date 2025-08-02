package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{AbstractDataType, DataType, DateType}
import org.apache.spark.unsafe.types.UTF8String

/**
  * Returns the first day of the month which the date belongs to.
  */
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the first day of the month which the date belongs to.",
  examples = """
    Examples:
      > SELECT _FUNC_('2009-01-12');
       2009-01-01
  """,
  group = "datetime_funcs",
  since = "0.0.2"
)
case class BeginningOfMonth(startDate: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def nullSafeEval(date: Any): Any = {
    val level = DateTimeUtils.parseTruncLevel(UTF8String.fromString("MONTH"))
    DateTimeUtils.truncDate(date.asInstanceOf[Int], level)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val level = DateTimeUtils.parseTruncLevel(UTF8String.fromString("MONTH"))
    val dtu   = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, sd => s"$dtu.parseTruncLevel($sd, $level)")
  }

  override def prettyName: String = "beginning_of_month"

  override protected def withNewChildInternal(newChild: Expression): BeginningOfMonth =
    copy(startDate = newChild)
}
