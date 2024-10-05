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

  private def schemaToSortedSelectExpr[A](schema: StructType, f: StructField => A)(implicit ord: Ordering[A]): Seq[Column] = {
    def handleNestedType(t: DataType, name: String, outerCol: Column, firstLevel: Boolean = false): Column =
      t match {
        case st: StructType =>
          struct(
            st.fields
              .sortBy(f)
              .map(field =>
                handleNestedType(
                  field.dataType,
                  field.name,
                  field.dataType match {
                    case StructType(_) | ArrayType(_: StructType, _) => outerCol(field.name)
                    case _                                           => outerCol
                  }
                ).as(field.name)
              ): _*
          ).as(name)
        case ArrayType(_, _)  => handleArrayType(t, name, outerCol).as(name)
        case _ if firstLevel  => outerCol
        case _ if !firstLevel => outerCol(name)
      }

    // For handling reordering of nested arrays
    def handleArrayType(t: DataType, name: String, outer: Column): Column =
      t match {
        case ArrayType(innerType: ArrayType, _) =>
          transform(outer, inner => handleArrayType(innerType, name, inner)).as(name)
        case ArrayType(innerType: StructType, _) =>
          transform(outer, inner => handleNestedType(innerType, name, inner).as(name)).as(name)
        case _ => outer.as(name)
      }

    val result = schema.fields.sortBy(f).foldLeft(Seq.empty[Column]) {
      case (acc, field) =>
        acc :+ handleNestedType(field.dataType, field.name, col(field.name), firstLevel = true)
    }
    result
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
