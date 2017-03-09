package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class MissingDataFrameColumnsException(smth: String) extends Exception(smth)

object DataFrameValidator {

  def missingColumns(df: DataFrame, requiredColNames: Seq[String]) = {
    requiredColNames diff df.columns.toSeq
  }

  def validatePresenceOfColumns(df: DataFrame, colNames: Seq[String]): Unit = {
    val m = missingColumns(df, colNames)
    if (m.nonEmpty) {
      throw new MissingDataFrameColumnsException(s"DataFrame must contain the following columns: ${m.mkString(", ")}")
    }
  }

}
