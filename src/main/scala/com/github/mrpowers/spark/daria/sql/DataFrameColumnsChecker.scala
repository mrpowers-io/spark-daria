package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class MissingDataFrameColumnsException(smth: String) extends Exception(smth)

class DataFrameColumnsChecker(df: DataFrame, requiredColNames: Seq[String]) {

  val missingColumns = requiredColNames.diff(df.columns.toSeq)

  def missingColumnsMessage(): String = {
    s"The [${missingColumns.mkString(", ")}] columns are not included in the DataFrame with the following columns [${df.columns.mkString(", ")}]"
  }

  def validatePresenceOfColumns(): Unit = {
    if (missingColumns.nonEmpty) {
      throw new MissingDataFrameColumnsException(missingColumnsMessage())
    }
  }

}

