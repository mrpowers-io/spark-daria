package com.github.mrpowers.spark.daria.sql

import org.apache.commons.text.WordUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object functions {

  /** Replaces all whitespace in a string with single spaces */
  def singleSpace(col: Column): Column = {
    trim(regexp_replace(col, " +", " "))
  }

  /** Removes all whitespace in a string */
  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

  /** Removes all whitespace in a string */
  def removeAllWhitespace(colName: String): Column = {
    regexp_replace(col(colName), "\\s+", "")
  }

  /** Deletes inner whitespace and leaves leading and trailing whitespace */
  def antiTrim(col: Column): Column = {
    regexp_replace(col, "\\b\\s+\\b", "")
  }

  /** Removes all non-word characters from a string */
  def removeNonWordCharacters(col: Column): Column = {
    regexp_replace(col, "[^\\w\\s]+", "")
  }

  /** Like Scala Array exists method, but for ArrayType columns */
  def exists[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] {
    (arr: Seq[T]) => arr.exists(f(_))
  }

  /** Like Scala Array forall method, but for ArrayType columns */
  def forall[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] {
    (arr: Seq[T]) => arr.forall(f(_))
  }

  /** Returns true if multiple columns are equal to a given value */
  def multiEquals[T: TypeTag](value: T, cols: Column*) = {
    cols.map(_.===(value)).reduceLeft(_.&&(_))
  }

  /** Returns the years between two dates */
  def yeardiff(end: Column, start: Column): Column = {
    datediff(end, start) / 365
  }

  def between(col: Column, min: Any, max: Any): Column = {
    col.geq(min) && col.leq(max)
  }

  /** Like initcap, but factors in additional delimiters */
  def capitalizeFully(delimiters: List[Char]) = {
    udf((s: String) => WordUtils.capitalizeFully(s, delimiters: _*))
  }

  /** Truncates the length of StringType columns */
  def truncate(col: Column, len: Int): Column = {
    substring(col, 0, len)
  }

  /** Like array but doesn't include null elements */
  def arrayExNull(cols: Column*): Column = {
    split(concat_ws(",,,", cols: _*), ",,,")
  }

}
