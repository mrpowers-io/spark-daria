package com.github.mrpowers.spark.daria.sql.types


import org.apache.spark.sql.types.StructField

sealed trait CustomStructField

case class LeftOnly(leftField: StructField) extends CustomStructField

case class Matched(field: StructField) extends CustomStructField

case class RightOnly(rightField: StructField) extends CustomStructField

case class CustomStructType(fields: Array[CustomStructField]) extends Seq[CustomStructField] {
  override def apply(idx: Int): CustomStructField = fields(idx)

  override def length: Int = fields.length

  override def iterator: Iterator[CustomStructField] = fields.iterator
}

