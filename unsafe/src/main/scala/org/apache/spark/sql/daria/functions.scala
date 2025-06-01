package org.apache.spark.sql.daria

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.expressions.{Expression, RandGamma}
import org.apache.spark.sql.functions.{lit, log, signum}
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
  def randGamma(seed: Long, shape: Double, scale: Double): Column = withExpr(RandGamma(seed, shape, scale)).alias("gamma_random")

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * @note The function is non-deterministic in general case.
   */
  def randGamma(seed: Column, shape: Column, scale: Column): Column = withExpr(RandGamma(seed.expr, shape.expr, scale.expr)).alias("gamma_random")

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * @note The function is non-deterministic in general case.
   */
  def randGamma(shape: Double, scale: Double): Column = randGamma(Utils.random.nextLong, shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * @note The function is non-deterministic in general case.
   */
  def randGamma(shape: Column, scale: Column): Column = randGamma(lit(Utils.random.nextLong), shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * @note The function is non-deterministic in general case.
   */
  def randGamma(): Column = randGamma(1.0, 1.0)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * An alias of `randGamma`
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(seed: Long, shape: Double, scale: Double): Column = randGamma(seed, shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   * An alias of `randGamma`
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(seed: Column, shape: Column, scale: Column): Column = randGamma(seed, shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * An alias of `randGamma`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(shape: Double, scale: Double): Column = randGamma(shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with the specified shape and scale parameters.
   *
   * An alias of `randGamma`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(shape: Column, scale: Column): Column = randGamma(shape, scale)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Gamma distribution with default parameters (shape = 1.0, scale = 1.0).
   *
   * An alias of `randGamma`
   *
   * @return A column with i.i.d. samples from the default Gamma distribution.
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_gamma(): Column = randGamma()

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randLaplace(seed: Long, mu: Column, beta: Column): Column = {
    val u = F.rand(seed) - lit(0.5)
    mu - beta * signum(u) * log(lit(1) - (lit(2) * F.abs(u)))
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randLaplace(seed: Long, mu: Double, beta: Double): Column = {
    randLaplace(seed, lit(mu), lit(beta))
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randLaplace(mu: Column, beta: Column): Column = randLaplace(Utils.random.nextLong, mu, beta)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randLaplace(mu: Double, beta: Double): Column = randLaplace(Utils.random.nextLong, mu, beta)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with default parameters (mu = 0.0, beta = 1.0).
   *
   * @note The function is non-deterministic in general case.
   */
  def randLaplace(): Column = randLaplace(0.0, 1.0)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * An alias of `randLaplace`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(seed: Long, mu: Double, beta: Double): Column = {
    randLaplace(seed, mu, beta)
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * An alias of `randLaplace`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(mu: Column, beta: Column): Column = randLaplace(mu, beta)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with the specified location parameter `mu` and scale parameter `beta`.
   *
   * An alias of `randLaplace`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(mu: Double, beta: Double): Column = randLaplace(mu, beta)

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples
   * from the Laplace distribution with default parameters (mu = 0.0, beta = 1.0).
   *
   *  An alias of `randLaplace`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_laplace(): Column = randLaplace()

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * @note The function is non-deterministic in general case.
   */
  def randRange(seed: Long, min: Column, max: Column): Column = {
    min + (max - min) * F.rand(seed)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * @note The function is non-deterministic in general case.
   */
  def randRange(seed: Long, min: Int, max: Int): Column = {
    randRange(seed, lit(min), lit(max))
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * @note The function is non-deterministic in general case.
   */
  def randRange(min: Int, max: Int): Column = {
    randRange(Utils.random.nextLong, min, max)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * @note The function is non-deterministic in general case.
   */
  def randRange(min: Column, max: Column): Column = {
    randRange(Utils.random.nextLong, min, max)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * An alias of `randRange`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_range(seed: Long, min: Column, max: Column): Column = {
    randRange(seed, min, max)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * An alias of `randRange`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_range(seed: Long, min: Int, max: Int): Column = {
    randRange(seed, min, max)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * An alias of `randRange`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_range(min: Int, max: Int): Column = {
    randRange(min, max)
  }

  /**
   * Generate a random column with independent and identically distributed (i.i.d.) samples
   * uniformly distributed in [`min`, `max`).
   *
   * An alias of `randRange`
   *
   * @note The function is non-deterministic in general case.
   */
  def rand_range(min: Column, max: Column): Column = {
    randRange(min, max)
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution with given `mean` and `variance`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randn(seed: Long, mean: Column, variance: Column): Column = {
    val stddev = F.sqrt(variance)
    F.randn(seed) * stddev + lit(mean)
  }

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution with given `mean` and `variance`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randn(seed: Long, mean: Double, variance: Double): Column = {
    randn(seed, lit(mean), lit(variance))
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

  /**
   * Generate a column with independent and identically distributed (i.i.d.) samples from
   * the standard normal distribution with given `mean` and `variance`.
   *
   * @note The function is non-deterministic in general case.
   */
  def randn(mean: Column, variance: Column): Column = {
    randn(Utils.random.nextLong, mean, variance)
  }

  /**
   * Asserts that the column is not null. If the column is null, it will throw an exception.
   * This will also update the nullability of the column to false.
   */
  def assertNotNull(column: Column): Column = withExpr(AssertNotNull(column.expr))

  /**
   * Asserts that the column is not null. If the column is null, it will throw an exception.
   * This will also update the nullability of the column to false.
   */
  def assert_not_null(column: Column): Column = assertNotNull(column)
}
