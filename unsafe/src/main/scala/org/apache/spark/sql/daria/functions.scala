package org.apache.spark.sql.daria

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, RandGamma}
import org.apache.spark.sql.functions.{lit, log, when}
import org.apache.spark.sql.{functions => F}
import org.apache.spark.util.Utils

object functions {
  private def withExpr(expr: Expression): Column = Column(expr)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(seed: Long, shape: Double, scale: Double): Column = withExpr(RandGamma(seed, shape, scale)).alias("gamma_random")

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(shape: Double, scale: Double): Column = rand_gamma(Utils.random.nextLong, shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with default parameters (shape = 1.0, scale = 1.0).
   *
   * @return A column with i.i.d. samples from the default Gamma distribution.
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(): Column = rand_gamma(1.0, 1.0)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(seed: Long, mu: Double, beta: Double): Column = {
    val mu_   = lit(mu)
    val beta_ = lit(beta)
    val u     = F.rand(seed)
    when(u < 0.5, mu_ + beta_ * log(lit(2) * u))
      .otherwise(mu_ - beta_ * log(lit(2) * (lit(1) - u)))
      .alias("laplace_random")
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(mu: Double, beta: Double): Column = rand_laplace(Utils.random.nextLong, mu, beta)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with default parameters (mu = 0.0, beta = 1.0).
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(): Column = rand_laplace(0.0, 1.0)

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_range(seed: Long, min: Int, max: Int): Column = {
    val min_ = lit(min)
    val max_ = lit(max)
    min_ + (max_ - min_) * F.rand(seed)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_range(min: Int, max: Int): Column = {
    rand_range(Utils.random.nextLong, min, max)
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution with given `mean` and `variance`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randn(seed: Long, mean: Double, variance: Double): Column = {
    val stddev = math.sqrt(variance)
    F.randn(seed) * lit(stddev) + lit(mean)
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution with given `mean` and `variance`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randn(mean: Double, variance: Double): Column = {
    randn(Utils.random.nextLong, mean, variance)
  }
}
