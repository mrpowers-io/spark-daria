package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, RandGamma}
import org.apache.spark.sql.functions.{lit, log, rand, when}

object customFunctions {
  private def withExpr(expr: Expression): Column = Column(expr)

  def randGamma(seed: Long, shape: Double, scale: Double): Column = withExpr(RandGamma(seed, shape, scale)).alias("gamma_random")

  def randLaplace(seed: Long, mu: Double, beta: Double): Column = {
    val mu_ = lit(mu)
    val beta_ = lit(beta)
    val u = rand(seed)
    when(u < 0.5, mu_ + beta_ * log(lit(2) * u))
      .otherwise(mu_ - beta_ * log(lit(2) * (lit(1) - u)))
      .alias("laplace_random")
  }
}
