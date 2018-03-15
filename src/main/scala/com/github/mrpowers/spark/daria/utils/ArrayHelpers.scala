package com.github.mrpowers.spark.daria.utils

object ArrayHelpers {

  /**
   * Escapes all the strings in an array an concatenates them into a single regexp string
   *
   */
  def regexpString(strs: Array[String]): String = {
    val t = strs.filter(_ != null).map { str: String =>
      StringHelpers.escapeForSqlRegexp(str).getOrElse(None)
    }
    t.mkString("|")
  }

}
