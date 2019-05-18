package com.github.mrpowers.spark.daria.sql.udafs

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

class ArrayConcat(elementSchema: DataType, nullable: Boolean = true) extends UserDefinedAggregateFunction {

  private val schema = StructType(
    List(
      StructField(
        "value",
        dataType,
        nullable
      )
    )
  )

  override def inputSchema: StructType = schema

  override def bufferSchema: StructType = schema

  override def dataType: DataType = ArrayType(elementSchema)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Seq.empty[Any]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val value = input.getAs[Seq[Any]](0)
    if (value != null) {
      buffer(0) = buffer.getAs[Seq[Any]](0) ++ value
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Seq[Any]](0) ++ buffer2.getAs[Seq[Any]](0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Seq[Any]](0)
  }
}
