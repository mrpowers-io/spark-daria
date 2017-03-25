package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

package object transformations {

  def snakeCaseColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumnRenamed(colName, toSnakeCase(colName))
    }
  }

  private def toSnakeCase(str: String): String = {
    str.toLowerCase().replace(" ", "_")
  }

}
