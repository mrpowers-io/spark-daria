package com.github.mrpowers.spark.daria.sql.types

import org.apache.spark.sql.types.StructField

object StructFieldHelpers {

  def customEquals(s1: StructField, s2: StructField, ignoreNullable: Boolean = false): Boolean = {
    if (ignoreNullable) {
      s1.name == s2.name &&
      s1.dataType == s2.dataType
    } else {
      s1.name == s2.name &&
      s1.dataType == s2.dataType &&
      s1.nullable == s2.nullable
    }
  }

  def prettyFormat(sf: StructField): String = {
    s"""StructField("${sf.name}", ${sf.dataType}, ${sf.nullable})"""
  }

}
