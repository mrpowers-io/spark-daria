package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

/**
 * Spark [has a ton of SQL functions](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) and spark-daria is meant to fill in any gaps.
 *
 * @groupname datetime_funcs Date time functions
 * @groupname string_funcs String functions
 * @groupname collection_funcs Collection functions
 * @groupname misc_funcs Misc functions
 * @groupname Ungrouped Support functions for DataFrames
 */
object functions {
  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
   * Replaces all whitespace in a string with single spaces
   *  {{{
   * val actualDF = sourceDF.withColumn(
   *   "some_string_single_spaced",
   *   singleSpace(col("some_string"))
   * )
   * }}}
   *
   * Replaces all multispaces with single spaces (e.g. changes `"this   has     some"` to `"this has some"`.
   *
   * @group string_funcs
   */
  def singleSpace(col: Column): Column = {
    trim(
      regexp_replace(
        col,
        " +",
        " "
      )
    )
  }

  /**
   * Removes all whitespace in a string
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "some_string_without_whitespace",
   *   removeAllWhitespace(col("some_string"))
   * )
   * }}}
   *
   * Removes all whitespace in a string (e.g. changes `"this   has     some"` to `"thishassome"`.
   *
   * @group string_funcs
   * @since 0.16.0
   */
  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(
      col,
      "\\s+",
      ""
    )
  }

  /**
   * Removes all whitespace in a string
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "some_string_without_whitespace",
   *   removeAllWhitespace(col("some_string"))
   * )
   * }}}
   *
   * @group string_funcs
   * @since 0.16.0
   */
  def removeAllWhitespace(colName: String): Column = {
    regexp_replace(
      col(colName),
      "\\s+",
      ""
    )
  }

  /**
   * Deletes inner whitespace and leaves leading and trailing whitespace
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "some_string_anti_trimmed",
   *   antiTrim(col("some_string"))
   * )
   * }}}
   *
   * Removes all inner whitespace, but doesn't delete leading or trailing whitespace (e.g. changes `"  this   has     some   "` to `"  thishassome   "`.
   *
   * @group string_funcs
   */
  def antiTrim(col: Column): Column = {
    regexp_replace(
      col,
      "\\b\\s+\\b",
      ""
    )
  }

  /**
   * Removes all non-word characters from a string
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "some_string_remove_non_word_chars",
   *   removeNonWordCharacters(col("some_string"))
   * )
   * }}}
   *
   * Removes all non-word characters from a string, excluding whitespace (e.g. changes `"  ni!!ce  h^^air person  "` to `"  nice  hair person  "`).
   *
   * @group string_funcs
   */
  def removeNonWordCharacters(col: Column): Column = {
    regexp_replace(
      col,
      "[^\\w\\s]+",
      ""
    )
  }

  /**
   * Like initcap, but factors in additional delimiters
   *
   * @group string_funcs
   */
  val capitalizeFully = udf[Option[String], String, String](capitalizeFullyFun)

  def capitalizeFullyFun(colName: String, delimiters: String): Option[String] = {
    val d = Option(delimiters).getOrElse(return None)
    val c = Option(colName).getOrElse(return None)
    // initialize the previousLetter to be the null character - the closest representation of the empty character: https://stackoverflow.com/questions/8306060/how-do-i-represent-an-empty-char-in-scala
    var previousLetter: Char = '\0'
    Some(c.map { (letter: Char) =>
      if (d.contains(previousLetter) || previousLetter.equals('\0')) {
        previousLetter = letter
        letter.toUpper
      } else {
        previousLetter = letter
        letter.toLower
      }
    })
  }

  /**
   * Truncates the length of StringType columns
   * {{{
   * sourceDF.withColumn(
   *   "some_string_truncated",
   *   truncate(col("some_string"), 3)
   * )
   * }}}
   *
   * Truncates the `"some_string"` column to only have three characters.
   *
   * @group string_funcs
   */
  def truncate(col: Column, len: Int): Column = {
    substring(
      col,
      0,
      len
    )
  }

  private def createLambda(f: Column => Column) = {
    val x        = UnresolvedNamedLambdaVariable(Seq("x"))
    val function = f(new Column(x)).expr
    LambdaFunction(
      function,
      Seq(x)
    )
  }

  private def createLambda(f: (Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq("x"))
    val y = UnresolvedNamedLambdaVariable(Seq("y"))
    val function = f(
      new Column(x),
      new Column(y)
    ).expr
    LambdaFunction(
      function,
      Seq(
        x,
        y
      )
    )
  }

  /**
   * Returns an array of elements after applying a tranformation to each element
   * in the input array.
   *
   * Suppose we have the following sourceDF:
   *
   * {{{
   * +---------+
   * |     nums|
   * +---------+
   * |[1, 4, 9]|
   * |[1, 3, 5]|
   * +---------+
   * }}}
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "squared", transform(col("nums"), x => x * 2)
   * )
   *
   * actualDF.show()
   *
   * +---------+-----------+
   * |     nums|    squared|
   * +---------+-----------+
   * |[1, 4, 9]|[1, 16, 81]|
   * |[1, 3, 5]| [1, 9, 25]|
   * +---------+-----------+
   * }}}
   *
   * @group collection_funcs
   */
  def transform(column: Column, f: Column => Column): Column = new Column(
    ArrayTransform(
      column.expr,
      createLambda(f)
    )
  )

  /**
   * Returns an array of elements after applying a tranformation to each element
   * in the input array with its index.
   *
   * Suppose we have the following sourceDF:
   *
   * {{{
   * +---------+
   * |     nums|
   * +---------+
   * |[1, 2, 3]|
   * +---------+
   * }}}
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "idx_sum", transform(col("nums"), (x, i) => x + i)
   * )
   *
   * actualDF.show()
   *
   * +---------+---------+
   * |     nums|  idx_sum|
   * +---------+---------+
   * |[1, 2, 3]|[1, 3, 5]|
   * +---------+---------+
   * }}}
   *
   * @group collection_funcs
   */
  def transform(column: Column, f: (Column, Column) => Column): Column = new Column(
    ArrayTransform(
      column.expr,
      createLambda(f)
    )
  )

  /**
   * Like Scala Array exists method, but for ArrayType columns
   * Scala has an Array#exists function that works like this:
   *
   * {{{
   * Array(1, 2, 5).exists(_ % 2 == 0) // true
   * }}}
   *
   * Suppose we have the following sourceDF:
   *
   * {{{
   * +---------+
   * |     nums|
   * +---------+
   * |[1, 4, 9]|
   * |[1, 3, 5]|
   * +---------+
   * }}}
   *
   * We can use the spark-daria `exists` function to see if there are even numbers in the arrays in the `nums` column.
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   * "nums_has_even",
   * exists[Int]((x: Int) => x % 2 == 0).apply(col("nums"))
   * )
   *
   * actualDF.show()
   *
   * +---------+-------------+
   * |     nums|nums_has_even|
   * +---------+-------------+
   * |[1, 4, 9]|         true|
   * |[1, 3, 5]|        false|
   * +---------+-------------+
   * }}}
   *
   * @group collection_funcs
   */
  def exists[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] { (arr: Seq[T]) =>
    arr.exists(f(_))
  }

  /**
   * Like Scala Array forall method, but for ArrayType columns
   *
   * Scala has an Array#forall function that works like this:
   *
   * Array("catdog", "crazy cat").forall(_.contains("cat")) // true
   *
   * Suppose we have the following sourceDF:
   *
   * {{{
   * +------------+
   * |       words|
   * +------------+
   * |[snake, rat]|
   * |[cat, crazy]|
   * +------------+
   * }}}
   *
   * We can use the spark-daria `forall` function to see if all the strings in an array contain the string `"cat"`.
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   * "all_words_begin_with_c",
   * forall[String]((x: String) => x.startsWith("c")).apply(col("words"))
   * )
   *
   * actualDF.show()
   *
   * +------------+----------------------+
   * |       words|all_words_begin_with_c|
   * +------------+----------------------+
   * |[snake, rat]|                 false|
   * |[cat, crazy]|                  true|
   * +------------+----------------------+
   * }}}
   *
   * @group collection_funcs
   */
  def forall[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] { (arr: Seq[T]) =>
    arr.forall(f(_))
  }

  /**
   * Like array() function but doesn't include null elements
   *
   * @group collection_funcs
   */
  def arrayExNull(cols: Column*): Column = {
    split(
      concat_ws(
        ",,,",
        cols: _*
      ),
      ",,,"
    )
  }

  /**
   * Returns true if multiple columns are equal to a given value
   *
   * Returns `true` if multiple columns are equal to a value.
   *
   * Suppose we have the following sourceDF:
   *
   * {{{
   * +---+---+
   * | s1| s2|
   * +---+---+
   * |cat|cat|
   * |cat|dog|
   * |pig|pig|
   * +---+---+
   * }}}
   *
   * We can use the `multiEquals` function to see if multiple columns are equal to `"cat"`.
   *
   * {{{
   * val actualDF = sourceDF.withColumn(
   *   "are_s1_and_s2_cat",
   *   multiEquals[String]("cat", col("s1"), col("s2"))
   * )
   *
   * actualDF.show()
   *
   * +---+---+-----------------+
   * | s1| s2|are_s1_and_s2_cat|
   * +---+---+-----------------+
   * |cat|cat|             true|
   * |cat|dog|            false|
   * |pig|pig|            false|
   * +---+---+-----------------+
   * }}}
   *
   * @group misc_funcs
   */
  def multiEquals[T: TypeTag](value: T, cols: Column*) = {
    cols.map(_.===(value)).reduceLeft(_.&&(_))
  }

  /**
   * Returns the number of years from `start` to `end`.
   * There is a `datediff` function that calculates the number of days between two dates, but there isn't a `yeardiff` function that calculates the number of years between two dates.
   *
   * The `com.github.mrpowers.spark.daria.sql.functions.yeardiff` function fills the gap.  Let's see how it works!
   *
   * Suppose we have the following `testDf`
   *
   * {{{
   * +--------------------+--------------------+
   * |      first_datetime|     second_datetime|
   * +--------------------+--------------------+
   * |2016-09-10 00:00:...|2001-08-10 00:00:...|
   * |2016-04-18 00:00:...|2010-05-18 00:00:...|
   * |2016-01-10 00:00:...|2013-08-10 00:00:...|
   * |                null|                null|
   * +--------------------+--------------------+
   * }}}
   *
   * We can run the `yeardiff` function as follows:
   *
   * {{{
   * import com.github.mrpowers.spark.daria.sql.functions._
   *
   * val actualDf = testDf
   *   .withColumn("num_years", yeardiff(col("first_datetime"), col("second_datetime")))
   *
   * actualDf.show()
   *
   * +--------------------+--------------------+------------------+
   * |      first_datetime|     second_datetime|         num_years|
   * +--------------------+--------------------+------------------+
   * |2016-09-10 00:00:...|2001-08-10 00:00:...|15.095890410958905|
   * |2016-04-18 00:00:...|2010-05-18 00:00:...| 5.923287671232877|
   * |2016-01-10 00:00:...|2013-08-10 00:00:...| 2.419178082191781|
   * |                null|                null|              null|
   * +--------------------+--------------------+------------------+
   * }}}
   *
   * @group datetime_funcs
   */
  def yeardiff(end: Column, start: Column): Column = {
    datediff(
      end,
      start
    ) / 365
  }

  def bucketFinder(col: Column,
                   buckets: Array[(Any, Any)],
                   inclusiveBoundries: Boolean = false,
                   lowestBoundLte: Boolean = false,
                   highestBoundGte: Boolean = false): Column = {

    val inclusiveBoundriesCol = lit(inclusiveBoundries)
    val lowerBoundLteCol      = lit(lowestBoundLte)
    val upperBoundGteCol      = lit(highestBoundGte)

    val b = buckets.map { res: (Any, Any) =>
      when(
        col.isNull,
        lit(null)
      ).when(
          lowerBoundLteCol === false && lit(res._1).isNull && lit(res._2).isNotNull && col < lit(res._2),
          lit(s"<${res._2}")
        )
        .when(
          lowerBoundLteCol === true && lit(res._1).isNull && lit(res._2).isNotNull && col <= lit(res._2),
          lit(s"<=${res._2}")
        )
        .when(
          upperBoundGteCol === false && lit(res._1).isNotNull && lit(res._2).isNull && col > lit(res._1),
          lit(s">${res._1}")
        )
        .when(
          upperBoundGteCol === true && lit(res._1).isNotNull && lit(res._2).isNull && col >= lit(res._1),
          lit(s">=${res._1}")
        )
        .when(
          inclusiveBoundriesCol === true && col.between(
            res._1,
            res._2
          ),
          lit(s"${res._1}-${res._2}")
        )
        .when(
          inclusiveBoundriesCol === false && col.gt(res._1) && col.lt(res._2),
          lit(s"${res._1}-${res._2}")
        )
    }

    coalesce(b: _*)

  }

  // copied from Rosetta Code: https://rosettacode.org/wiki/Luhn_test_of_credit_card_numbers#Scala
  private[sql] def isLuhn(str: String): Option[Boolean] = {
    val s = Option(str).getOrElse(return None)
    if (s.isEmpty()) {
      return Some(false)
    }
    var (odd, sum) = (true, 0)

    for (int <- str.reverse.map { _.toString.toShort }) {
      if (odd) sum += int
      else sum += (int * 2 % 10) + (int / 5)
      odd = !odd
    }
    Some(sum % 10 == 0)
  }

  val isLuhnNumber = udf[Option[Boolean], String](isLuhn)

  private[sql] def findAllInString(str: String, regexp: String): Option[Array[String]] = {
    val r = regexp.r
    val s = Option(str).getOrElse(return None)
    if (s.isEmpty()) {
      return Some(Array[String]())
    }
    Some(r.findAllIn(s).toArray)
  }

  val regexp_extract_all = udf[Option[Array[String]], String, String](findAllInString)

  def array_groupBy[T: TypeTag](f: (T => Boolean)) = udf[Option[Map[Boolean, Seq[T]]], Seq[T]] { (arr: Seq[T]) =>
    if (arr == null) {
      null
    } else {
      Some(arr.groupBy(f(_)))
    }
  }

}
