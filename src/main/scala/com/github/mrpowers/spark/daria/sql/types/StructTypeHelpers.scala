package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object StructTypeHelpers {

  def flattenSchema(schema: StructType, delimiter: String = ".", prefix: String = null): Array[Column] = {
    schema.fields.flatMap(structField => {
      val codeColName =
        if (prefix == null) structField.name
        else prefix + "." + structField.name
      val colName =
        if (prefix == null) structField.name
        else prefix + delimiter + structField.name

      structField.dataType match {
        case st: StructType =>
          flattenSchema(
            schema = st,
            delimiter = delimiter,
            prefix = colName
          )
        case _ => Array(col(codeColName).alias(colName))
      }
    })
  }

}
