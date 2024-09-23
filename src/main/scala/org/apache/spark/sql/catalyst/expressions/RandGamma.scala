package org.apache.spark.sql.catalyst.expressions

import org.apache.commons.math3.distribution.GammaDistribution
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedSeed
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.util.random.XORShiftRandomAdapted

case class RandGamma(child: Expression, shape: Expression, scale: Expression, hideSeed: Boolean = false) extends TernaryExpression
  with ExpectsInputTypes
  with Stateful
  with ExpressionWithRandomSeed {

  override def seedExpression: Expression = child

  @transient protected lazy val seed: Long = seedExpression match {
    case e if e.dataType == IntegerType => e.eval().asInstanceOf[Int]
    case e if e.dataType == LongType => e.eval().asInstanceOf[Long]
  }

  @transient protected lazy val shapeVal: Double = shape.dataType match {
    case IntegerType => shape.eval().asInstanceOf[Int]
    case LongType => shape.eval().asInstanceOf[Long]
    case FloatType | DoubleType => shape.eval().asInstanceOf[Double]
  }

  @transient protected lazy val scaleVal: Double = scale.dataType match {
    case IntegerType => scale.eval().asInstanceOf[Int]
    case LongType => scale.eval().asInstanceOf[Long]
    case FloatType | DoubleType => scale.eval().asInstanceOf[Double]
  }

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    distribution = new GammaDistribution(new XORShiftRandomAdapted(seed + partitionIndex), shapeVal, scaleVal)
  }
  @transient private var distribution: GammaDistribution = _

  def this() = this(UnresolvedSeed, Literal(1.0, DoubleType), Literal(1.0, DoubleType), true)

  def this(child: Expression, shape: Expression, scale: Expression) = this(child, shape, scale, false)

  override def withNewSeed(seed: Long): RandGamma = RandGamma(Literal(seed, LongType), shape, scale, hideSeed)

  override protected def evalInternal(input: InternalRow): Double = distribution.sample()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val distributionClassName = classOf[GammaDistribution].getName
    val rngClassName = classOf[XORShiftRandomAdapted].getName
    val disTerm = ctx.addMutableState(distributionClassName, "distribution")
    ctx.addPartitionInitializationStatement(
      s"$disTerm = new $distributionClassName(new $rngClassName(${seed}L + partitionIndex), $shapeVal, $scaleVal);")
    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $disTerm.sample();""",
      isNull = FalseLiteral)
  }

  override def freshCopy(): RandGamma = RandGamma(child, shape, scale, hideSeed)

  override def flatArguments: Iterator[Any] = Iterator(child, shape, scale)

  override def prettyName: String = "rand_gamma"

  override def sql: String = s"rand_gamma(${if (hideSeed) "" else s"${child.sql}, ${shape.sql}, ${scale.sql}"})"

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, DoubleType, DoubleType)

  override def dataType: DataType = DoubleType

  override def first: Expression = child

  override def second: Expression = shape

  override def third: Expression = scale

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    copy(child = newFirst, shape = newSecond, scale = newThird)
}

object RandGamma {
  def apply(seed: Long, shape: Double, scale: Double): RandGamma = RandGamma(Literal(seed, LongType), Literal(shape, DoubleType), Literal(scale, DoubleType))
}
