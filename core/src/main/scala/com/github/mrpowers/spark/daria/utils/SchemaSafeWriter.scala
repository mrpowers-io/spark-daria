package com.github.mrpowers.spark.daria.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

case class DariaSchemaMismatchError(smth: String) extends Exception(smth)

object SchemaSafeWriter {

  // writes to a Parquet data lake if the schema matches the existing schema
  // throws an error if the schemas don't match
  def parquetAppend(path: String, df: DataFrame): Unit = {
    append(path, df, "parquet")
  }

  def deltaAppend(path: String, df: DataFrame): Unit = {
    append(path, df, "delta")
  }

  def append(path: String, df: DataFrame, fileFormat: String): Unit = {
    val spark          = SparkSession.getActiveSession.get
    val existingDF     = spark.read.parquet(path)
    val existingSchema = existingDF.schema
    if (existingSchema.equals(df.schema)) {
      df.write.format(fileFormat).mode("append").save(path)
    } else {
      println("Existing schema:")
      existingDF.printSchema()
      println("New schema:")
      df.printSchema()
      throw DariaSchemaMismatchError(s"The new schema doesn't match the existing schema")
    }
  }

}
