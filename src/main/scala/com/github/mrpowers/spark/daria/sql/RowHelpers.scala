package com.github.mrpowers.spark.daria.sql

import java.lang
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row

object RowHelpers {

  /**
   * Some utilities to avoid class cast exception while retrieving
   * typed cells out of a row.
   *
   * @param row
   * @tparam T
   */
  implicit class PimpedRow[T](val row: Row) extends AnyVal {

    /**
     * retrieves an Int providing meaningful conversions
     *
     * @param fieldIndex
     * @return
     */
    def safeGetInt(fieldIndex: Int): Int = {
      val value = row.get(fieldIndex)
      value match {
        case a: java.lang.Integer    => a.toInt
        case a: java.lang.Long       => a.toInt
        case a: java.lang.Byte       => a.toInt
        case a: java.lang.Short      => a.toInt
        case a: java.math.BigDecimal => a.intValue()
        case a: java.lang.String     => a.toInt
        case a: java.sql.Timestamp   => a.getTime.toInt
        case a: java.sql.Date        => a.getTime.toInt

        /**
         * because null.asInstanceOf[Int] gives.....0 !
         */
        case null => throw new NullPointerException(s"$fieldIndex")
        case _    => value.asInstanceOf[Int]
      }
    }

    /**
     * retrieves an Int providing meaningful conversions
     *
     * @param fieldName
     * @return
     */
    def safeGetInt(fieldName: String): Int = safeGetInt(row.fieldIndex(fieldName))

    /**
     * retrieves a long providing a meaningful conversion
     *
     * @param fieldIndex
     * @return
     */
    def safeGetLong(fieldIndex: Int): Long = {
      val value = row.get(fieldIndex)
      value match {
        case a: Integer              => a.toLong
        case a: lang.Long            => a.toLong
        case a: lang.Byte            => a.toLong
        case a: lang.Short           => a.toLong
        case a: java.math.BigDecimal => a.longValue()
        case a: String               => a.toLong
        case a: Timestamp            => a.getTime
        case a: Date                 => a.getTime

        /**
         * because null.asInstanceOf[Int] gives.....0 !
         */
        case null => throw new NullPointerException(s"$fieldIndex")
        case _    => value.asInstanceOf[Long]
      }
    }

    /**
     * retrieves a long providing a meaningful conversion
     *
     * @param fieldName
     * @return
     */
    def safeGetLong(fieldName: String): Long = safeGetLong(row.fieldIndex(fieldName))

    /**
     * Retrieves a string providing a meaningful conversion.
     *
     * @param fieldIndex
     * @return
     */
    def safeGetString(fieldIndex: Int): String = {
      val value = row.get(fieldIndex)
      if (value == null) null else s"$value"
    }

    /**
     * retrieves a string providing a meaningful conversion
     *
     * @param fieldName
     * @return
     */
    def safeGetString(fieldName: String): String = safeGetString(row.fieldIndex(fieldName))

    /**
     * retrieves a timestamp providing a meaningful conversion
     *
     * @param fieldIndex
     * @return
     */
    def safeGetTimestamp(fieldIndex: Int): Timestamp = {
      val value = row.get(fieldIndex)
      if (value == null) null
      else {
        value match {
          case a: java.lang.Long => new Timestamp(a)
          case a: java.lang.String =>
            Timestamp.valueOf(a)
          case a: java.sql.Date =>
            new Timestamp(a.getTime)
          case a: java.sql.Timestamp =>
            a
          case _ => value.asInstanceOf[Timestamp]
        }
      }
    }

    /**
     * retrieves a timestamp providing a meaningful conversion
     *
     * @param fieldName
     * @return
     */
    def safeGetTimestamp(fieldName: String): Timestamp = safeGetTimestamp(row.fieldIndex(fieldName))

    /**
     * retrieves a date providing a meaningful conversion
     *
     * @param fieldIndex
     * @return
     */
    def safeGetDate(fieldIndex: Int): Date = {
      val value = row.get(fieldIndex)
      if (value == null) null
      else {
        value match {
          case a: java.lang.Long => new Date(a)
          case a: java.lang.String =>
            Date.valueOf(a)
          case a: java.sql.Date =>
            a
          case a: java.sql.Timestamp =>
            new Date(a.getTime)
          case _ => value.asInstanceOf[Date]
        }
      }
    }

    /**
     * retrieves a date providing a meaningful conversion
     *
     * @param fieldIndex
     * @return
     */
    def safeGetDate(fieldIndex: Int, dateFormat: String): Date = {
      val value = row.get(fieldIndex)
      if (value == null) null
      else {
        value match {
          case a: java.lang.Long => new Date(a)
          case a: java.lang.String =>
            new java.sql.Date(new SimpleDateFormat(dateFormat).parse(a).getTime)
          case a: java.sql.Date =>
            a
          case a: java.sql.Timestamp =>
            new Date(a.getTime)
          case _ => value.asInstanceOf[Date]
        }
      }
    }

    /**
     * retrieves a timestamp providing a meaningful conversion
     *
     * @param fieldName
     * @return
     */
    def safeGetDate(fieldName: String, dateFormat: String): Date = safeGetDate(row.fieldIndex(fieldName), dateFormat)

    /**
     * retrieves a timestamp providing a meaningful conversion
     *
     * @param fieldName
     * @return
     */
    def safeGetDate(fieldName: String): Date = safeGetDate(row.fieldIndex(fieldName))

    /**
     * retrieves an Double providing meaningful conversions
     *
     * @param fieldIndex
     * @return
     */
    def safeGetDouble(fieldIndex: Int): Double = {
      val value = row.get(fieldIndex)
      value match {
        case a: java.lang.Integer    => a.toDouble
        case a: java.lang.Long       => a.toDouble
        case a: java.lang.Byte       => a.toDouble
        case a: java.lang.Short      => a.toDouble
        case a: java.math.BigDecimal => a.doubleValue()
        case a: java.lang.String     => a.toDouble
        case a: java.sql.Timestamp   => a.getTime.toDouble
        case a: java.sql.Date        => a.getTime.toDouble

        /**
         * because null.asInstanceOf[Int] gives.....0 !
         */
        case null => throw new NullPointerException(s"$fieldIndex")
        case _    => value.asInstanceOf[Double]
      }
    }

    /**
     * retrieves a double providing a meaningful conversion
     *
     * @param fieldName
     * @return
     */
    def safeGetDouble(fieldName: String): Double = safeGetDouble(row.fieldIndex(fieldName))

  }
}
