package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

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

}
