package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

package object transformations {

  def snakeCaseColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (acc, cn) =>
      acc.withColumnRenamed(cn, toSnakeCase(cn))
    }
  }

  private def toSnakeCase(s: String): String = {
    s.toLowerCase().replace(" ", "_")
  }

}
