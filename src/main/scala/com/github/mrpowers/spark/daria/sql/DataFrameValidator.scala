package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait DataFrameValidator {

  /** Throws an error if the DataFrame doesn't contain all the required columns */
  def validatePresenceOfColumns(df: DataFrame, requiredColNames: Seq[String]): Unit = {
    val c = new DataFrameColumnsChecker(df, requiredColNames)
    c.validatePresenceOfColumns()
  }

  /** Throws an error if the DataFrame schema doesn't match the required schema */
  def validateSchema(df: DataFrame, requiredSchema: StructType): Unit = {
    val c = new DataFrameSchemaChecker(df, requiredSchema)
    c.validateSchema()
  }

  /** Throws an error if the DataFrame contains any of the prohibited columns */
  def validateAbsenceOfColumns(df: DataFrame, prohibitedColNames: Seq[String]): Unit = {
    val c = new DataFrameColumnsAbsence(df, prohibitedColNames)
    c.validateAbsenceOfColumns()
  }

}
