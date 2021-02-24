package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql._

case class ProhibitedDataFrameColumnsException(smth: String) extends Exception(smth)

private[sql] class DataFrameColumnsAbsence(df: DataFrame, prohibitedColNames: Seq[String]) {

  val extraColNames = (df.columns.toSeq).intersect(prohibitedColNames)

  def extraColumnsMessage(): String = {
    val extra = extraColNames.mkString(", ")
    val all   = df.columns.mkString(", ")
    s"The [${extra}] columns are not allowed to be included in the DataFrame with the following columns [${all}]"
  }

  def validateAbsenceOfColumns(): Unit = {
    if (extraColNames.nonEmpty) {
      throw ProhibitedDataFrameColumnsException(extraColumnsMessage())
    }
  }

}
