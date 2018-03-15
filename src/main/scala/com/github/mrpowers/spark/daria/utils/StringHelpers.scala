package com.github.mrpowers.spark.daria.utils

object StringHelpers {

  /**
   * Escapes all the special characters in a string for a SQL regexp expression
   *
   */
  def escapeForSqlRegexp(str: String): Option[String] = {
    val s = Option(str).getOrElse(return None)
    val charsToEscape: List[String] = List("\\(", "\\)", "\\|")
    Some(charsToEscape.foldLeft(str) {
      case (res, pattern) =>
        res.replaceAll(pattern, "\\" + pattern)
    })
  }

}
