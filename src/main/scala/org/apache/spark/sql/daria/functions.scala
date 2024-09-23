package org.apache.spark.sql.daria

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, RandGamma}
import org.apache.spark.sql.functions.{lit, log, rand, when}
import org.apache.spark.util.Utils

object functions {
  private def withExpr(expr: Expression): Column = Column(expr)

  def randGamma(seed: Long, shape: Double, scale: Double): Column = withExpr(RandGamma(seed, shape, scale)).alias("gamma_random")
  def randGamma(shape: Double, scale: Double): Column             = randGamma(Utils.random.nextLong, shape, scale)
  def randGamma(): Column                                         = randGamma(1.0, 1.0)

  def randLaplace(seed: Long, mu: Double, beta: Double): Column = {
    val mu_   = lit(mu)
    val beta_ = lit(beta)
    val u     = rand(seed)
    when(u < 0.5, mu_ + beta_ * log(lit(2) * u))
      .otherwise(mu_ - beta_ * log(lit(2) * (lit(1) - u)))
      .alias("laplace_random")
  }

  def randLaplace(mu: Double, beta: Double): Column = randLaplace(Utils.random.nextLong, mu, beta)
  def randLaplace(): Column                         = randLaplace(0.0, 1.0)
}
