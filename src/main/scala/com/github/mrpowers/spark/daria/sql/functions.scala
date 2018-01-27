package com.github.mrpowers.spark.daria.sql

import org.apache.commons.text.WordUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

/**
 * Functions available for DataFrame operations.
 *
 * @groupname datetime_funcs Date time functions
 * @groupname string_funcs String functions
 * @groupname collection_funcs Collection functions
 * @groupname misc_funcs Misc functions
 * @groupname Ungrouped Support functions for DataFrames
 */

object functions {

  /**
   * Replaces all whitespace in a string with single spaces
   *
   * @group string_funcs
   */
  def singleSpace(col: Column): Column = {
    trim(regexp_replace(col, " +", " "))
  }

  /**
   * Removes all whitespace in a string
   *
   * @group string_funcs
   * @since 0.16.0
   */
  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

  /**
   * Removes all whitespace in a string
   *
   * @group string_funcs
   * @since 0.16.0
   */
  def removeAllWhitespace(colName: String): Column = {
    regexp_replace(col(colName), "\\s+", "")
  }

  /**
   * Deletes inner whitespace and leaves leading and trailing whitespace
   *
   * @group string_funcs
   */
  def antiTrim(col: Column): Column = {
    regexp_replace(col, "\\b\\s+\\b", "")
  }

  /**
   * Removes all non-word characters from a string
   *
   * @group string_funcs
   */
  def removeNonWordCharacters(col: Column): Column = {
    regexp_replace(col, "[^\\w\\s]+", "")
  }

  /**
   * Like initcap, but factors in additional delimiters
   *
   * @group string_funcs
   */
  def capitalizeFully(delimiters: List[Char]) = {
    udf((s: String) => WordUtils.capitalizeFully(s, delimiters: _*))
  }

  /**
   * Truncates the length of StringType columns
   *
   * @group string_funcs
   */
  def truncate(col: Column, len: Int): Column = {
    substring(col, 0, len)
  }

  /**
   * Like Scala Array exists method, but for ArrayType columns
   *
   * @group collection_funcs
   */
  def exists[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] {
    (arr: Seq[T]) => arr.exists(f(_))
  }

  /**
   * Like Scala Array forall method, but for ArrayType columns
   *
   * @group collection_funcs
   */
  def forall[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] {
    (arr: Seq[T]) => arr.forall(f(_))
  }

  /**
   * Like array but doesn't include null elements
   *
   * @group collection_funcs
   */
  def arrayExNull(cols: Column*): Column = {
    split(concat_ws(",,,", cols: _*), ",,,")
  }

  /**
   * Returns true if multiple columns are equal to a given value
   *
   * @group misc_funcs
   */
  def multiEquals[T: TypeTag](value: T, cols: Column*) = {
    cols.map(_.===(value)).reduceLeft(_.&&(_))
  }

  /**
   * Returns the number of years from `start` to `end`.
   *
   * @group datetime_funcs
   */
  def yeardiff(end: Column, start: Column): Column = {
    datediff(end, start) / 365
  }

}
