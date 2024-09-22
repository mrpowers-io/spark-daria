package com.github.mrpowers.spark.daria.sql.udafs

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

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

case class ArrayConcatAggregator[T: TypeTag]() extends Aggregator[Seq[T], Seq[T], Seq[T]] {
  override def zero: Seq[T] = Seq.empty[T]

  override def reduce(b: Seq[T], a: Seq[T]): Seq[T] = {
    if (a == null) {
      return b
    }
    b ++ a
  }

  override def merge(b1: Seq[T], b2: Seq[T]): Seq[T] = {
    if (b2 == null) {
      return b1
    }
    b1 ++ b2
  }

  override def finish(reduction: Seq[T]): Seq[T] = reduction

  override def bufferEncoder: Encoder[Seq[T]] = ExpressionEncoder[Seq[T]]()

  override def outputEncoder: Encoder[Seq[T]] = ExpressionEncoder[Seq[T]]()
}
