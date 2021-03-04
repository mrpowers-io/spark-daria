package com.github.mrpowers.spark.daria.utils

import org.apache.spark.sql.Row

object RowHelpers {

  def prettyPrintRow(r: Row): Seq[String] = {
    def prettyType(value: Any): String = {
      value match {
        case l: Long   => l.toString + "L"
        case i: Int    => i.toString
        case s: String => s""""$s""""
        case d: Double => d.toString
        case null      => null
      }
    }
    val res = r.toSeq.map { (value: Any) => prettyType(value) }
    res
  }

}
