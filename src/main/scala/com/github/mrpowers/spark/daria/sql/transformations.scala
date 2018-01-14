package com.github.mrpowers.spark.daria.sql

import com.github.mrpowers.spark.daria.sql.DataFrameExt._
import com.github.mrpowers.spark.daria.sql.functions.truncate
import org.apache.commons.text.WordUtils
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame}

case class InvalidColumnSortOrderException(smth: String) extends Exception(smth)

object transformations {

  /** Sorts the columns of a DataFrame alphabetically */
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

  /** snake_cases all the columns of a DataFrame */
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

  /** Title Cases all the columns of a DataFrame */
  def titleCaseColumns()(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(colName, WordUtils.capitalizeFully(colName))
    }
  }

  /** Runs regexp_replace on multiple columns */
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

  /** Runs regexp_replace on all StringType columns in a DataFrame */
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

  /** Truncates multiple columns in a DataFrame */
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
