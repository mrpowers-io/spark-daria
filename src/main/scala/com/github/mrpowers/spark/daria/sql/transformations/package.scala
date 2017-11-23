package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.StructField
import com.github.mrpowers.spark.daria.sql.functions.truncate
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

case class InvalidColumnSortOrderException(smth: String) extends Exception(smth)

package object transformations {

  def sortColumns(order: String = "asc")(df: DataFrame): DataFrame = {
    val cols = if (order == "asc") {
      df.columns.sorted
    } else if (order == "desc") {
      df.columns.sorted.reverse
    } else {
      val message = s"The sort order must be 'asc' or 'desc'.  Your sort order was '$order'."
      throw new InvalidColumnSortOrderException(message)
    }
    df.select(cols.map(col): _*)
  }

  def snakeCaseColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(colName, toSnakeCase(colName))
    }
  }

  private def toSnakeCase(str: String): String = {
    str.toLowerCase().replace(" ", "_")
  }

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
