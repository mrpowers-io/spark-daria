package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructField, StructType}
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

}
