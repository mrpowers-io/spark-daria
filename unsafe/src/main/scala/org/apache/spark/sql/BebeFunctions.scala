package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.functions.{expr, lit, when}
import org.apache.spark.util.Utils
import org.apache.spark.sql.classic.ShimColumnConversions.ShimColumnConversionsOps

/**
 * @groupname string_funcs String Functions
 * @groupname agg_funcs Aggregate Functions
 * @groupname math_funcs Math Functions
 */
object BebeFunctions {

  private def withExpr(expr: Expression): Column = Column(expr)

  private def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false
  ): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  // ADDITIONAL UTILITY FUNCTIONS

  def beginningOfDay(col: Column): Column =
    withExpr {
      TruncTimestamp(lit("day").toExpression, col.toExpression, None)
    }

  def beginningOfDay(col: Column, timeZoneId: String): Column =
    withExpr {
      TruncTimestamp(lit("day").toExpression, col.toExpression, Some(timeZoneId))
    }

  def beginningOfMonth(col: Column): Column =
    withExpr {
      BeginningOfMonth(col.toExpression)
    }

  def endOfDay(col: Column): Column =
    endOfDay(col, None)

  def endOfDay(col: Column, timeZoneId: String): Column =
    endOfDay(col, Some(timeZoneId))

  def endOfDay(col: Column, timeZoneId: Option[String] = None): Column =
    withExpr {
      val startOfNextDay =
        TruncTimestamp(lit("day").toExpression, DateAdd(col.toExpression, lit(1).toExpression), timeZoneId)
      // 1 second before end-of-day to match Rails: https://apidock.com/rails/DateTime/end_of_day
      TimeAdd(startOfNextDay, expr("interval -1 second").toExpression, timeZoneId)
    }

  def endOfMonth(col: Column): Column =
    withExpr {
      val startOfNextMonth = TruncTimestamp(lit("month").toExpression, AddMonths(col.toExpression, lit(1).toExpression))
      DateAdd(startOfNextMonth, lit(-1).toExpression)
    }

  // FUNCTIONS MISSING IN SCALA API

  /**
   * Returns the approximate percentile value of numeric
   * column `col` at the given percentage. The value of percentage must be between 0.0
   *  and 1.0. The `accuracy` parameter (default: 10000) is a positive numeric literal which
   *  controls approximation accuracy at the cost of memory. Higher value of `accuracy` yields
   *  better accuracy, `1.0/accuracy` is the relative error of the approximation.
   *  When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
   *  In this case, returns the approximate percentile array of column `col` at the given
   *  percentage array.
   *
   *  @group agg_funcs
   */
  def bebe_approx_percentile(col: Column, percentage: Column, accuracy: Column): Column =
    withAggregateFunction {
      new ApproximatePercentile(col.toExpression, percentage.toExpression, accuracy.toExpression)
    }

  /**
   * Returns the approximate percentile value of numeric
   * column `col` at the given percentage. The value of percentage must be between 0.0
   *  and 1.0.
   *  When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
   *  In this case, returns the approximate percentile array of column `col` at the given
   *  percentage array.
   *
   *  @group agg_funcs
   */
  def bebe_approx_percentile(col: Column, percentage: Column): Column =
    withAggregateFunction {
      new ApproximatePercentile(col.toExpression, percentage.toExpression)
    }

  /**
   * Returns length of array or map.
   *
   * The function returns null for null input if spark.sql.legacy.sizeOfNull is set to false or
   * spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1 for null input.
   * With the default settings, the function returns -1 for null input.
   *
   * @group collection_funcs
   */
  def bebe_cardinality(col: Column): Column = withExpr { Size(col.toExpression) }

  /**
   * Returns the cotangent of `expr`, as if computed by `java.lang.Math.cot`.
   *
   * @param col the column of which to compute the cotangent
   */
  def bebe_cot(col: Column): Column = withExpr(Cot(col.toExpression))

  /**
   * Returns the number of `TRUE` values for the expression.
   *
   * @group agg_funcs
   *
   * @param col the expression to conditionally count
   */
  def bebe_count_if(col: Column): Column =
    withAggregateFunction {
      CountIf(col.toExpression)
    }

  /**
   * Returns the number of characters
   *
   * @group string_funcs
   */
  def bebe_character_length(col: Column): Column =
    withExpr {
      Length(col.toExpression)
    }

  /**
   * Returns ASCII character
   *
   * Returns the ASCII character having the binary equivalent to expr. If n is larger than 256 the result is equivalent to chr(n % 256)
   *
   * @group string_funcs
   */
  def bebe_chr(col: Column): Column = withExpr { Chr(col.toExpression) }

  /**
   * Returns Euler's number, e
   */
  def bebe_e(): Column = withExpr { EulerNumber() }

  /**
   * ifnull(expr1, expr2) - Returns expr2 if expr1 is null, or expr1 otherwise.
   */
  def bebe_if_null(col1: Column, col2: Column): Column =
    withExpr {
      Coalesce(Seq(col1.toExpression, col2.toExpression))
    }

  /**
   * inline(expr) - Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.
   */
  def bebe_inline(col: Column): Column =
    withExpr {
      Inline(col.toExpression)
    }

  /**
   * True if the current expression is NOT null.
   */
  def bebe_is_not_null(col: Column): Column = withExpr { IsNotNull(col.toExpression) }

  /**
   * left(str, len) - Returns the leftmost len(len can be string type) characters from the string str, if len is less or equal than 0 the result is an empty string.
   */
  def bebe_left(col: Column, len: Column): Column =
    withExpr {
      BebeLeft(col, len)
    }

  /**
   * str like pattern[ ESCAPE escape] - Returns true if str matches pattern with escape, null if any arguments are null, false otherwise.
   */
  def bebe_like(col: Column, sqlLike: Column): Column =
    withExpr {
      Like(col.toExpression, sqlLike.toExpression, '\\')
    }

  /**
   * make_date(year, month, day) - Create date from year, month and day fields.
   *
   * Arguments:
   *
   * year - the year to represent, from 1 to 9999
   * month - the month-of-year to represent, from 1 (January) to 12 (December)
   * day - the day-of-month to represent, from 1 to 31
   */
  def bebe_make_date(year: Column, month: Column, day: Column): Column =
    withExpr {
      MakeDate(year.toExpression, month.toExpression, day.toExpression)
    }

  /**
   * make_timestamp(year, month, day, hour, min, sec[, timezone]) - Create timestamp from year, month, day, hour, min, sec and timezone fields.
   *
   * Arguments:
   *
   * year - the year to represent, from 1 to 9999
   * month - the month-of-year to represent, from 1 (January) to 12 (December)
   * day - the day-of-month to represent, from 1 to 31
   * hour - the hour-of-day to represent, from 0 to 23
   * min - the minute-of-hour to represent, from 0 to 59
   * sec - the second-of-minute and its micro-fraction to represent, from 0 to 60. If the sec argument equals to 60, the seconds field is set to 0 and 1 minute is added to the final timestamp.
   * timezone - the time zone identifier. For example, CET, UTC and etc.
   */
  def bebe_make_timestamp(
      year: Column,
      month: Column,
      day: Column,
      hour: Column,
      min: Column,
      sec: Column
  ): Column =
    withExpr {
      MakeTimestamp(year.toExpression, month.toExpression, day.toExpression, hour.toExpression, min.toExpression, sec.toExpression)
    }

  /**
   * nvl2(expr1, expr2, expr3) - Returns expr2 if expr1 is not null, or expr3 otherwise.
   */
  def bebe_nvl2(col1: Column, col2: Column, col3: Column): Column =
    withExpr {
      Nvl2(col1.toExpression, col2.toExpression, col3.toExpression, If(IsNotNull(col1.toExpression), col2.toExpression, col3.toExpression))
    }

  /**
   * Returns the byte length of string data or number of bytes of binary data.
   */
  def bebe_octet_length(col: Column): Column =
    withExpr {
      OctetLength(col.toExpression)
    }

  /**
   * stack(n, expr1, ..., exprk) - Separates expr1, ..., exprk into n rows. Uses column names col0, col1, etc. by default unless specified otherwise.
   */
  def bebe_stack(col: Column, cols: Column*): Column =
    withExpr {
      Stack(col.toExpression +: cols.map(_.toExpression))
    }

  /**
   * parse_url(url, partToExtract) - Extracts a part from a URL.
   */
  def bebe_parse_url(col: Column, partToExtract: Column): Column =
    withExpr {
      ParseUrl(Seq(col.toExpression, partToExtract.toExpression))
    }

  /**
   * parse_url(url, partToExtract, urlParamKey) - Extracts a URL parameter value.
   */
  def bebe_parse_url(col: Column, partToExtract: Column, urlParamKey: Column): Column =
    withExpr {
      ParseUrl(Seq(col.toExpression, partToExtract.toExpression, urlParamKey.toExpression))
    }

  /**
   * percentile(col, percentage [, frequency]) - Returns the exact percentile value of numeric column col at the given percentage. The value of percentage must be between 0.0 and 1.0. The value of frequency should be positive integral
   *
   * percentile(col, array(percentage1 [, percentage2]...) [, frequency]) - Returns the exact percentile value array of numeric column col at the given percentage(s). Each value of the percentage array must be between 0.0 and 1.0. The value of frequency should be positive integral
   */
  def bebe_percentile(col: Column, percentage: Column): Column =
    withAggregateFunction {
      Percentile(col.toExpression, percentage.toExpression, Literal(1L))
    }

  /**
   * Extract all strings in the `str` that match the `regexp` expression
   * and corresponding to the regex group index.
   * @group string_funcs
   * @since 0.1.0
   */
  def bebe_regexp_extract_all(col: Column, regex: Column, groupIndex: Column): Column =
    withExpr {
      RegExpExtractAll(col.toExpression, regex.toExpression, groupIndex.toExpression)
    }

  /**
   * right(str, len) - Returns the rightmost len(len can be string type) characters from the string str,if len is less or equal than 0 the result is an empty string.
   */
  def bebe_right(col: Column, len: Column): Column =
    withExpr {
      BebeRight(col, len)
    }

  /**
   * sentences(str[, lang, country]) - Splits str into an array of array of words.
   */
  def bebe_sentences(col: Column): Column =
    withExpr {
      Sentences(col.toExpression, Literal(""), Literal(""))
    }

  /**
   * space(n) - Returns a string consisting of n spaces.
   */
  def bebe_space(col: Column): Column =
    withExpr {
      StringSpace(col.toExpression)
    }

  /**
   * substr(str, pos) - Returns the substring of str that starts at pos, or the slice of byte array that starts at pos.
   */
  def bebe_substr(col: Column, pos: Column): Column =
    withExpr {
      Substring(col.toExpression, pos.toExpression, Literal(Integer.MAX_VALUE))
    }

  /**
   * substr(str, pos, len) - Returns the substring of str that starts at pos and is of length len, or the slice of byte array that starts at pos and is of length len.
   */
  def bebe_substr(col: Column, pos: Column, len: Column): Column =
    withExpr {
      Substring(col.toExpression, pos.toExpression, len.toExpression)
    }

  /**
   * uuid() - Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.
   */
  def bebe_uuid(): Column =
    withExpr {
      Uuid()
    }

  /**
   * weekday(date) - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
   */
  def bebe_weekday(col: Column): Column =
    withExpr {
      WeekDay(col.toExpression)
    }

  /**
   * Builder for case when that prevents coding errors
   *
   * @param cases a vector of previous added cases
   */
  case class WhenB(private val cases: Vector[(Column, Any)] = Vector.empty) {

    private def casesToWhenColumn: Column = {
      val head = cases.head
      cases.tail.foldLeft(when(head._1, head._2))((w, c) => w.when(c._1, c._2))
    }

    def caseW(condition: Column, value: Any): WhenB =
      WhenB(cases :+ (condition, value))

    def otherwise(column: Any): Column =
      if (cases.isEmpty)
        lit(column)
      else
        casesToWhenColumn.otherwise(column)

    def otherwiseNull: Column =
      if (cases.isEmpty)
        lit(null)
      else
        casesToWhenColumn
  }

  val whenBuilder: WhenB = WhenB()

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
  def randGamma(seed: Column, shape: Column, scale: Column): Column =
    withExpr(RandGamma(seed.toExpression, shape.toExpression, scale.toExpression)).alias("gamma_random")

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
    mu - beta * F.signum(u) * F.log(lit(1) - (lit(2) * F.abs(u)))
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
  def assertNotNull(column: Column): Column = withExpr(AssertNotNull(column.toExpression))

  /**
   * Asserts that the column is not null. If the column is null, it will throw an exception.
   * This will also update the nullability of the column to false.
   */
  def assert_not_null(column: Column): Column = assertNotNull(column)

  /**
   * Changes the nullability of the column to true, indicating that the column can contain null values.
   */
  private[sql] def knownNullable(column: Column): Column = withExpr(KnownNullable(column.toExpression))

  /**
   * Changes the nullability of the column to false, indicating that the column can contain null values.
   */
  private[sql] def knownNotNull(column: Column): Column = withExpr(KnownNotNull(column.toExpression))
}
