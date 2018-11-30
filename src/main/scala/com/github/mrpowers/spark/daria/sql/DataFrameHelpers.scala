package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object DataFrameHelpers extends DataFrameValidator {

  /**
   * Converts two column to a map of key value pairs
   *
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * Converts two columns in a DataFrame to a Map.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +-----------+---------+
   * |     island|fun_level|
   * +-----------+---------+
   * |    boracay|        7|
   * |long island|        9|
   * +-----------+---------+
   * }}}
   *
   * Let's convert this DataFrame to a Map with `island` as the key and `fun_level` as the value.
   *
   * {{{
   * val actual = DataFrameHelpers.twoColumnsToMap[String, Integer](
   *   sourceDF,
   *   "island",
   *   "fun_level"
   * )
   *
   * println(actual)
   *
   * // Map(
   * //   "boracay" -> 7,
   * //   "long island" -> 9
   * // )
   * }}}
   */
  def twoColumnsToMap[keyType: TypeTag, valueType: TypeTag](
    df: DataFrame,
    keyColName: String,
    valueColName: String
  ): Map[keyType, valueType] = {
    validatePresenceOfColumns(
      df,
      Seq(
        keyColName,
        valueColName
      )
    )
    df.select(
        keyColName,
        valueColName
      )
      .collect()
      .map(r => (r(0).asInstanceOf[keyType], r(1).asInstanceOf[valueType]))
      .toMap
  }

  /**
   * Converts a DataFrame column to an Array of values
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * This function converts a column to an array of items.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +---+
   * |num|
   * +---+
   * |  1|
   * |  2|
   * |  3|
   * +---+
   * }}}
   *
   * Let's convert the `num` column to an Array of values.  Let's run the code and view the results.
   *
   * {{{
   * val actual = DataFrameHelpers.columnToArray[Int](sourceDF, "num")
   *
   * println(actual)
   *
   * // Array(1, 2, 3)
   * }}}
   */
  def columnToArray[T: ClassTag](df: DataFrame, colName: String): Array[T] = {
    df.select(colName).collect().map(r => r(0).asInstanceOf[T])
  }

  /**
   * Converts a DataFrame column to a List of values
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * This function converts a column to a list of items.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +---+
   * |num|
   * +---+
   * |  1|
   * |  2|
   * |  3|
   * +---+
   * }}}
   *
   * Let's convert the `num` column to a List of values.  Let's run the code and view the results.
   *
   * {{{
   * val actual = DataFrameHelpers.columnToList[Int](sourceDF, "num")
   *
   * println(actual)
   *
   * // List(1, 2, 3)
   * }}}
   */
  def columnToList[T: ClassTag](df: DataFrame, colName: String): List[T] = {
    columnToArray[T](
      df,
      colName
    ).toList
  }

  /**
   * Converts a DataFrame to an Array of Maps
   * '''N.B. This method uses `collect` and should only be called on small DataFrames.'''
   *
   * Converts a DataFrame to an array of Maps.
   *
   * Suppose we have the following `sourceDF`:
   *
   * {{{
   * +----------+-----------+---------+
   * |profession|some_number|pay_grade|
   * +----------+-----------+---------+
   * |    doctor|          4|     high|
   * |   dentist|         10|     high|
   * +----------+-----------+---------+
   * }}}
   *
   * Run the code to convert this DataFrame into an array of Maps.
   *
   * {{{
   * val actual = DataFrameHelpers.toArrayOfMaps(sourceDF)
   *
   * println(actual)
   *
   * Array(
   *   Map("profession" -> "doctor", "some_number" -> 4, "pay_grade" -> "high"),
   *   Map("profession" -> "dentist", "some_number" -> 10, "pay_grade" -> "high")
   * )
   * }}}
   */
  def toArrayOfMaps(df: DataFrame) = {
    df.collect.map(r => Map(df.columns.zip(r.toSeq): _*))
  }

}
