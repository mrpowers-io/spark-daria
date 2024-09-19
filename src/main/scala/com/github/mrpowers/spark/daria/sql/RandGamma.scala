package com.github.mrpowers.spark.daria.sql

import org.apache.commons.math3.distribution.GammaDistribution
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedSeed
import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionWithRandomSeed, Literal, RDG, Stateful, UnaryExpression}
import org.apache.spark.sql.types._

case class RandGamma(child: Expression, shape: Double = 1.0, scale: Double = 1.0, hideSeed: Boolean = false) extends RDG {
  override protected def initializeInternal(partitionIndex: Int): Unit = {
    distribution = new GammaDistribution(new XORShiftRandomAdapted(seed + partitionIndex), shape, scale)
  }
  @transient private var distribution: GammaDistribution = _

  def this() = this(UnresolvedSeed, 1.0, 1.0, true)

  def this(child: Expression) = this(child, 1.0, 1.0, false)

  override def withNewSeed(seed: Long): RandGamma = RandGamma(Literal(seed, LongType), shape, scale, hideSeed)

  override protected def evalInternal(input: InternalRow): Double = distribution.sample()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val distributionClassName = classOf[GammaDistribution].getName
    val rngClassName = classOf[XORShiftRandomAdapted].getName
    val disTerm = ctx.addMutableState(distributionClassName, "distribution")
    ctx.addPartitionInitializationStatement(
      s"$disTerm = new $distributionClassName(new $rngClassName(${seed}L + partitionIndex), $shape, $scale);")
    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $disTerm.sample();""",
      isNull = FalseLiteral)
  }

  override def freshCopy(): RandGamma = RandGamma(child, shape, scale, hideSeed)

  override def flatArguments: Iterator[Any] = Iterator(child)
  override def sql: String = {
    s"rand_gamma(${if (hideSeed) "" else child.sql})"
  }

  override protected def withNewChildInternal(newChild: Expression): RandGamma = copy(child = newChild)
}

object RandGamma {
  def apply(seed: Long): RandGamma = RandGamma(Literal(seed, LongType))
}

