package com.github.mrpowers.spark.daria.utils

object StringHelpers {

  /**
   * All the characters that need to be escaped for SQL regexp
   */
  val sqlCharsToEscape = "()/-.'|+".map { c: Char =>
    "\\" + c
  }.toList

  /**
   * Escapes all the special characters in a string for a SQL regexp expression
   * Better to simply use triple quotes
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

  /**
   * Copied from the Lyft Framework https://github.com/lift/framework/blob/master/core/util/src/main/scala/net/liftweb/util/StringHelpers.scala
   *
   * Turn a string of format "FooBar" into snake case "foo_bar"
   *
   * Note: snakify is not reversible, ie. in general the following will _not_ be true:
   *
   * s == camelify(snakify(s))
   *
   * @return the underscored string
   */
  def snakify(name: String): String =
    name
      .replaceAll(
        "([A-Z]+)([A-Z][a-z])",
        "$1_$2"
      )
      .replaceAll(
        "([a-z\\d])([A-Z])",
        "$1_$2"
      )
      .toLowerCase

  def camelCaseToSnakeCase(str: String): String = {
    str
      .replaceAll(
        "([A-Z]+)",
        "_$1"
      )
      .toLowerCase
      .stripPrefix("_")
  }

  def parquetCompatibleColumnName(str: String): String = {
    str.replaceAll("[,;{}()\n\t=]", "").replaceAll(" ", "_").toLowerCase()
  }

}
