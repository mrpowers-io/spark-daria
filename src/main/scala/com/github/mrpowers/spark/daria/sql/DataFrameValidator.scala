package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

trait DataFrameValidator {

  def validatePresenceOfColumns(df: DataFrame, requiredColNames: Seq[String]): Unit = {
    val c = new DataFrameColumnsChecker(df, requiredColNames)
    c.validatePresenceOfColumns()
  }

  def validateSchema(df: DataFrame, requiredSchema: StructType): Unit = {
    val c = new DataFrameSchemaChecker(df, requiredSchema)
    c.validateSchema()
  }

}
