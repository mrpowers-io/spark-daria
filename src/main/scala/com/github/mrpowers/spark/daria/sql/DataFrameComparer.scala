package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class DataFrameSchemaMismatch(smth: String) extends Exception(smth)
case class DataFrameContentMismatch(smth: String) extends Exception(smth)

trait DataFrameComparer {

  def schemaMismatchMessage(actualDF: DataFrame, expectedDF: DataFrame): String = {
    s"""
    Actual Schema:
    ${actualDF.schema}
    Expected Schema:
    ${expectedDF.schema}
     """
  }

  def contentMismatchMessage(actualDF: DataFrame, expectedDF: DataFrame): String = {
    s"""
    Actual DataFrame Content:
    ${DataFramePrettyPrint.showString(actualDF, 5)}
    Expected DataFrame Content:
    ${DataFramePrettyPrint.showString(expectedDF, 5)}
     """
  }

  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    if (!actualDF.schema.equals(expectedDF.schema)) {
      throw new DataFrameSchemaMismatch(schemaMismatchMessage(actualDF, expectedDF))
    }
    if (!actualDF.collect().sameElements(expectedDF.collect())) {
      throw new DataFrameContentMismatch(contentMismatchMessage(actualDF, expectedDF))
    }
  }

}
