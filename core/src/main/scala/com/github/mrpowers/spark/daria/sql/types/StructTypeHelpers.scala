package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object StructTypeHelpers {

  def build[U <: Product](fields: U*) = {
    fields.map {
      case x: StructField => x.asInstanceOf[StructField]
      case (name: String, dataType: DataType, nullable: Boolean) =>
        StructField(
          name,
          dataType,
          nullable
        )
    }
  }

  def flattenSchema(schema: StructType, baseField: String = "", flattenArrayType: Boolean = false): Seq[Column] = {
    schema.fields.foldLeft(Seq.empty[Column]) { case(acc, field) =>
      val colName = if (baseField.isEmpty) field.name else s"$baseField.${field.name}"
      field.dataType match {
        case t: StructType =>
          acc ++ flattenSchema(t, colName)
        case ArrayType(t: StructType, _) if flattenArrayType =>
          acc ++ flattenSchema(t, colName)
        case _ =>
          acc :+ col(colName)
      }
    }
  }

  private def schemaToSortedSelectExpr[A](schema: StructType, f: StructField => A, baseField: String = "")(implicit ord: Ordering[A]): Seq[Column] = {
    schema.fields.sortBy(f).foldLeft(Seq.empty[Column]) { case(acc, field) =>
      val colName = if (baseField.isEmpty) field.name else s"$baseField.${field.name}"
      field.dataType match {
        case t: StructType =>
          acc :+ struct(schemaToSortedSelectExpr(t, f, colName): _*).as(field.name)
        case ArrayType(t: StructType, _) =>
          acc :+ arrays_zip(schemaToSortedSelectExpr(t, f, colName): _*).as(field.name)
        case _ =>
          acc :+ col(colName)
      }
    }
  }

  /**
   * gets a StructType from a Scala type and
   * transforms field names from camel case to snake case
   */
  def schemaFor[T: TypeTag]: StructType = {
    val struct = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]

    struct.copy(fields = struct.fields.map { field =>
      field.copy(name = com.github.mrpowers.spark.daria.utils.StringHelpers.camelCaseToSnakeCase(field.name))
    })
  }

  implicit class StructTypeOps(schema: StructType) {
    def toSortedSelectExpr[A](f: StructField => A)(implicit ord: Ordering[A]): Seq[Column] = {
      schemaToSortedSelectExpr(schema, f)
    }
  }
}
