package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

case class InvalidDataFrameSchemaException(smth: String) extends Exception(smth)

private[sql] class DataFrameSchemaChecker(df: DataFrame, requiredSchema: StructType) {
  private def diff(required: Seq[StructField], schema: StructType): Seq[StructField] = {
    required.filterNot(isPresentIn(schema))
  }

  private def isPresentIn(schema: StructType)(reqField: StructField): Boolean = {
    Try(schema(reqField.name)) match {
      case Success(namedField) =>
        val basicMatch =
          namedField.name == reqField.name &&
            namedField.nullable == reqField.nullable &&
            namedField.metadata == reqField.metadata

        val contentMatch = reqField.dataType match {
          case reqSchema: StructType =>
            namedField.dataType match {
              case fieldSchema: StructType =>
                diff(reqSchema, fieldSchema).isEmpty
              case _ => false
            }
          case _ => reqField == namedField
        }

        basicMatch && contentMatch
      case Failure(_) => false
    }
  }

  val missingStructFields: Seq[StructField] = diff(requiredSchema, df.schema)

  def missingStructFieldsMessage(): String = {
    s"The [${missingStructFields.mkString(", ")}] StructFields are not included in the DataFrame with the following StructFields [${df.schema.toString()}]"
  }

  def validateSchema(): Unit = {
    if (missingStructFields.nonEmpty) {
      throw InvalidDataFrameSchemaException(missingStructFieldsMessage())
    }
  }

}
