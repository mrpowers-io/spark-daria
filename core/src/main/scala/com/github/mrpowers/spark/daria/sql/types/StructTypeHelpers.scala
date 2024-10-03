package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
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

  def flattenSchema(schema: StructType, prefix: String = ""): Array[Column] = {
    schema.fields.flatMap(structField => {
      val codeColName =
        if (prefix.isEmpty) structField.name
        else prefix + "." + structField.name

      structField.dataType match {
        case st: StructType =>
          flattenSchema(
            schema = st,
            prefix = codeColName
          )
        case _ => Array(col(codeColName))
      }
    })
  }

  private def schemaToSortedSelectExpr[A](schema: StructType, f: StructField => A, baseField: String = "")(implicit ord: Ordering[A]): Seq[Column] = {
    def handleDataType(t: DataType, colName: String, simpleName: String): Column =
      t match {
        case st: StructType =>
          struct(schemaToSortedSelectExpr(st, f, colName): _*).as(simpleName)
        case ArrayType(_, _) =>
          handleArrayType(t, col(colName), simpleName).as(simpleName)
        case _ =>
          col(colName)
      }

    // For handling reordering of nested arrays
    def handleArrayType(t: DataType, innerCol: Column, simpleName: String): Column =
      t match {
        case ArrayType(innerType: ArrayType, _) => transform(innerCol, outer => handleArrayType(innerType, outer, simpleName))
        case ArrayType(innerType: StructType, _) =>
          val cols = schemaToSortedSelectExpr(innerType, f)
          transform(innerCol, innerCol1 => struct(cols.map(c => innerCol1.getField(c.toString).as(c.toString)): _*))
        case _ => innerCol
      }

    schema.fields.sortBy(f).foldLeft(Seq.empty[Column]) {
      case (acc, field) =>
        val colName = if (baseField.isEmpty) field.name else s"$baseField.${field.name}"
        acc :+ handleDataType(field.dataType, colName, field.name)
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
    def toSortedSelectExpr[A](f: StructField => A)(implicit ord: Ordering[A]): Seq[Column] = schemaToSortedSelectExpr(schema, f)
  }
}
