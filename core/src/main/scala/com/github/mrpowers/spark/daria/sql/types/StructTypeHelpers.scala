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
    def childFieldToCol(childFieldType: DataType, childFieldName: String, parentCol: Column, firstLevel: Boolean = false): Column =
      childFieldType match {
        case st: StructType =>
          struct(
            st.fields
              .sortBy(f)
              .map(field =>
                childFieldToCol(
                  field.dataType,
                  field.name,
                  field.dataType match {
                    case StructType(_) | ArrayType(_: StructType, _) => parentCol(field.name)
                    case _                                           => parentCol
                  }
                ).as(field.name)
              ): _*
          ).as(childFieldName)
        case ArrayType(innerType, _)  =>
          transform(parentCol, childCol => childFieldToCol(innerType, childFieldName, childCol)).as(childFieldName)
        case _ if firstLevel  => parentCol
        case _ if !firstLevel => parentCol(childFieldName)
      }

    schema.fields.sortBy(f).foldLeft(Seq.empty[Column]) {
      case (acc, field) =>
        acc :+ childFieldToCol(field.dataType, field.name, col(field.name), firstLevel = true)
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
