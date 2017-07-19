package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataFrameHelpers {

  def toArrayOfMaps(df: DataFrame) = {
    df.collect.map(r => Map(df.columns.zip(r.toSeq): _*))
  }

  def withValueLookup(
    df: DataFrame,
    maps: Array[Map[String, Any]],
    mapValue: String,
    keyMappings: Map[String, String]
  ): DataFrame = {
    df.withColumn(
      mapValue,
      lit("hi")
    )
  }

}
