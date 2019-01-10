package com.github.mrpowers.spark.daria.utils

object StringHelpers {

  /**
   * All the characters that need to be escaped for SQL regexp
   *
   */
  val sqlCharsToEscape = "()/-.'|+".map { c: Char =>
    "\\" + c
  }.toList

  /**
   * Escapes all the special characters in a string for a SQL regexp expression
   * Better to simply use triple quotes ;)
   *
   */
  def escapeForSqlRegexp(str: String, charsToEscape: List[String] = sqlCharsToEscape): Option[String] = {

    val s = Option(str).getOrElse(return None)

    Some(charsToEscape.foldLeft(str) {
      case (res, pattern) =>
        res.replaceAll(
          pattern,
          "\\" + pattern
        )
    })
  }

  def toSnakeCase(str: String): String = {
    str
      .replaceAll(
        "\\s+",
        "_"
      )
      .toLowerCase
  }

  def camelCaseToSnakeCase(str: String): String = {
    str
      .replaceAll(
        "([A-Z]+)",
        "_$1"
      )
      .toLowerCase
      .stripPrefix("_")
  }

}
