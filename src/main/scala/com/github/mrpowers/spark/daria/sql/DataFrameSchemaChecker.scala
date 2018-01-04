package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

private[sql] case class InvalidDataFrameSchemaException(smth: String) extends Exception(smth)

private[sql] class DataFrameSchemaChecker(df: DataFrame, requiredSchema: StructType) {

  val missingStructFields = requiredSchema.diff(df.schema)

  def missingStructFieldsMessage(): String = {
    s"The [${missingStructFields.mkString(", ")}] StructFields are not included in the DataFrame with the following StructFields [${df.schema.toString()}]"
  }

  def validateSchema(): Unit = {
    if (missingStructFields.nonEmpty) {
      throw new InvalidDataFrameSchemaException(missingStructFieldsMessage())
    }
  }

}

