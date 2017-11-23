package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.commons.text.WordUtils

import scala.reflect.runtime.universe._

package object functions {

  def singleSpace(col: Column): Column = {
    trim(regexp_replace(col, " +", " "))
  }

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

  def antiTrim(col: Column): Column = {
    regexp_replace(col, "\\b\\s+\\b", "")
  }

  def removeNonWordCharacters(col: Column): Column = {
    regexp_replace(col, "[^\\w\\s]+", "")
  }

  def exists[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] {
    (arr: Seq[T]) => arr.exists(f(_))
  }

  def forall[T: TypeTag](f: (T => Boolean)) = udf[Boolean, Seq[T]] {
    (arr: Seq[T]) => arr.forall(f(_))
  }

  def multiEquals[T: TypeTag](value: T, cols: Column*) = {
    cols.map(_.===(value)).reduceLeft(_.&&(_))
  }

  def yeardiff(end: Column, start: Column): Column = {
    datediff(end, start) / 365
  }

  def between(col: Column, min: Any, max: Any): Column = {
    col.geq(min) && col.leq(max)
  }

  def rpadDaria(len: Integer, pad: String)(col: Column): Column = {
    rpad(col, len, pad)
  }

  def capitalizeFully(delimiters: List[Char]) = {
    udf((s: String) => WordUtils.capitalizeFully(s, delimiters: _*))
  }

  def truncate(col: Column, len: Int): Column = {
    substring(col, 0, len)
  }

}
