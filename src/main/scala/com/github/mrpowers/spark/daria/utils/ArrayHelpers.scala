package com.github.mrpowers.spark.daria.utils

object ArrayHelpers {

  /**
   * Escapes all the strings in an array an concatenates them into a single regexp string
   */
  def regexpString(strs: Array[String], charsToEscape: List[String] = StringHelpers.sqlCharsToEscape): String = {
    val t = strs.filter(_ != null).map { str: String =>
      StringHelpers
        .escapeForSqlRegexp(
          str,
          charsToEscape
        )
        .getOrElse(None)
    }
    t.mkString("|")
  }

}
