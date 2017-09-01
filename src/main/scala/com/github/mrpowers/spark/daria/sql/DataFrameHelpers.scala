package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import scala.reflect.runtime.universe._
import org.apache.spark.sql.functions._

object DataFrameHelpers extends DataFrameValidator {

  def twoColumnsToMap[keyType: TypeTag, valueType: TypeTag](
    df: DataFrame,
    keyColName: String,
    valueColName: String
  ): collection.mutable.Map[keyType, valueType] = {
    validatePresenceOfColumns(df, Seq(keyColName, valueColName))
    df
      .select(keyColName, valueColName)
      .collect()
      .foldLeft(collection.mutable.Map.empty[keyType, valueType]) { (memo, arr) =>
        memo += (arr(0).asInstanceOf[keyType] -> arr(1).asInstanceOf[valueType])
      }
  }

  def toArrayOfMaps(df: DataFrame) = {
    df.collect.map(r => Map(df.columns.zip(r.toSeq): _*))
  }

}
