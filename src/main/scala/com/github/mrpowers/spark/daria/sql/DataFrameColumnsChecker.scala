package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class MissingDataFrameColumnsException(smth: String) extends Exception(smth)

private[sql] class DataFrameColumnsChecker(df: DataFrame, requiredColNames: Seq[String]) {

  val missingColumns = requiredColNames.diff(df.columns.toSeq)

  def missingColumnsMessage(): String = {
    val missingColNames = missingColumns.mkString(", ")
    val allColNames     = df.columns.mkString(", ")
    s"The [${missingColNames}] columns are not included in the DataFrame with the following columns [${allColNames}]"
  }

  def validatePresenceOfColumns(): Unit = {
    if (missingColumns.nonEmpty) {
      throw MissingDataFrameColumnsException(missingColumnsMessage())
    }
  }

}
