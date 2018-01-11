package com.github.mrpowers.spark.daria.sql

import org.apache.commons.text.WordUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object functions {

  def singleSpace(col: Column): Column = {
    trim(regexp_replace(col, " +", " "))
  }

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

  def removeAllWhitespace(colName: String): Column = {
    regexp_replace(col(colName), "\\s+", "")
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

  def capitalizeFully(delimiters: List[Char]) = {
    udf((s: String) => WordUtils.capitalizeFully(s, delimiters: _*))
  }

  def truncate(col: Column, len: Int): Column = {
    substring(col, 0, len)
  }

}
