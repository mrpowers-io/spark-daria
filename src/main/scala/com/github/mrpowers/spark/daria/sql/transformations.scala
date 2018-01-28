package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import com.github.mrpowers.spark.daria.sql.functions.truncate
import org.apache.commons.text.WordUtils
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame}

case class InvalidColumnSortOrderException(smth: String) extends Exception(smth)

/**
 * Functions available for DataFrame operations.
 *
 * SQL transformations take a DataFrame as an argument and return a DataFrame.  They are suitable arguments for the `Dataset#transform` method.
 *
 * It's convenient to work with DataFrames that have snake_case column names.  Column names with spaces make it harder to write SQL queries.
 */

object transformations {

  /**
   * Sorts the columns of a DataFrame alphabetically
   * The `sortColumns` transformation sorts the columns in a DataFrame alphabetically.
   *
   * Suppose you start with the following `sourceDF`:
   *
   * {{{
   * +-----+---+-----+
   * | name|age|sport|
   * +-----+---+-----+
   * |pablo|  3| polo|
   * +-----+---+-----+
   * }}}
   *
   * Run the code:
   *
   * {{{
   * val actualDF = sourceDF.transform(sortColumns())
   * }}}
   *
   * Here’s the `actualDF`:
   *
   * {{{
   * +---+-----+-----+
   * |age| name|sport|
   * +---+-----+-----+
   * |  3|pablo| polo|
   * +---+-----+-----+
   * }}}
   */
  def sortColumns(order: String = "asc")(df: DataFrame): DataFrame = {
    val cols = if (order == "asc") {
      df.columns.sorted
    } else if (order == "desc") {
      df.columns.sorted.reverse
    } else {
      val message = s"The sort order must be 'asc' or 'desc'.  Your sort order was '$order'."
      throw new InvalidColumnSortOrderException(message)
    }
    df.reorderColumns(cols)
  }

  /**
   * snake_cases all the columns of a DataFrame
   * spark-daria defines a `com.github.mrpowers.spark.daria.sql.transformations.snakeCaseColumns` transformation to convert all the column names to snake\_case.
   *
   * import com.github.mrpowers.spark.daria.sql.transformations._
   *
   * {{{
   * val sourceDf = Seq(
   *   ("funny", "joke")
   * ).toDF("A b C", "de F")
   *
   * val actualDf = sourceDf.transform(snakeCaseColumns)
   *
   * actualDf.show()
   *
   * +-----+----+
   * |a_b_c|de_f|
   * +-----+----+
   * |funny|joke|
   * +-----+----+
   * }}}
   */
  def snakeCaseColumns()(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(colName, toSnakeCase(colName))
    }
  }

  private def toSnakeCase(str: String): String = {
    str
      .replaceAll("\\s+", "_")
      .toLowerCase
  }

  /**
   * Title Cases all the columns of a DataFrame
   */
  def titleCaseColumns()(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(colName, WordUtils.capitalizeFully(colName))
    }
  }

  /**
   * Runs regexp_replace on multiple columns
   * {{{
   * val actualDF = sourceDF.transform(
   *   transformations.multiRegexpReplace(
   *     List(col("person"), col("phone")),
   *     "cool",
   *     "dude"
   *   )
   * )
   * }}}
   *
   * Replaces all `"cool"` strings in the `person` and `phone` columns with the string `"dude"`.
   */
  def multiRegexpReplace(
    cols: List[Column],
    pattern: String = "\u0000",
    replacement: String = ""
  )(df: DataFrame): DataFrame = {
    cols.foldLeft(df) { (memoDF, col) =>
      memoDF
        .withColumn(
          col.toString(),
          regexp_replace(col, pattern, replacement)
        )
    }
  }

  /**
   * Runs regexp_replace on all StringType columns in a DataFrame
   * {{{
   * val actualDF = sourceDF.transform(
   *   transformations.bulkRegexpReplace(
   *     "cool",
   *     "dude"
   *   )
   * )
   * }}}
   *
   * Replaces all `"cool"` strings in all the `sourceDF` columns of `StringType` with the string `"dude"`.
   */
  def bulkRegexpReplace(
    pattern: String = "\u0000",
    replacement: String = ""
  )(df: DataFrame): DataFrame = {
    val cols = df.schema.filter { (s: StructField) =>
      s.dataType.simpleString == "string"
    }.map { (s: StructField) =>
      col(s.name)
    }.toList

    multiRegexpReplace(cols, pattern, replacement)(df)
  }

  /**
   * Truncates multiple columns in a DataFrame
   * {{{
   * val columnLengths: Map[String, Int] = Map(
   *   "person" -> 2,
   *   "phone" -> 3
   * )
   *
   * sourceDF.transform(
   *   truncateColumns(columnLengths)
   * )
   * }}}
   *
   * Limits the `"person"` column to 2 characters and the `"phone"` column to 3 characters.
   */
  def truncateColumns(
    columnLengths: Map[String, Int]
  )(df: DataFrame): DataFrame = {
    columnLengths.foldLeft(df) {
      case (memoDF, (colName, length)) =>
        if (memoDF.containsColumn(colName)) {
          memoDF.withColumn(colName, truncate(col(colName), length))
        } else {
          memoDF
        }
    }
  }

}
