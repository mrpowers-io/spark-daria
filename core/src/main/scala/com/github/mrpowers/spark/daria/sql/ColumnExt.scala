package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
 * Additional methods for the Spark Column class
 *
 * @groupname expr_ops Expression operators
 * @groupname df_ops DataFrame functions
 * @groupname Ungrouped Support functions for DataFrames
 *
 * @since 0.0.1
 */
object ColumnExt {

  implicit class ColumnMethods(col: Column) {

    /**
     * Chains column functions
     * //The chain method takes a org.apache.spark.sql.functions function as an argument and can be used as follows:
     * {{{
     * val wordsDf = Seq(
     *   ("Batman  "),
     *   ("  CATWOMAN"),
     *   (" pikachu ")
     * ).toDF("word")
     *
     * val actualDf = wordsDf.withColumn(
     *    "cleaned_word",
     *    col("word").chain(lower).chain(trim)
     * )
     *
     * actualDf.show()
     * +----------+------------+
     * |      word|cleaned_word|
     * +----------+------------+
     * |  Batman  |      batman|
     * |  CATWOMAN|    catwoman|
     * |  pikachu |     pikachu|
     * +----------+------------+
     * }}}
     *
     * @group expr_ops
     */
    def chain(colMethod: (Column => Column)): Column = {
      colMethod(col)
    }

    /**
     * Chains UDFs
     * {{{
     * def appendZ(s: String): String = {
     *   s + "Z"
     * }
     *
     * spark.udf.register("appendZUdf", appendZ _)
     *
     * def prependA(s: String): String = {
     *   "A" + s
     * }
     *
     * spark.udf.register("prependAUdf", prependA _)
     *
     * val hobbiesDf = Seq(
     *   ("dance"),
     *   ("sing")
     * ).toDF("word")
     *
     * val actualDf = hobbiesDf.withColumn(
     *   "fun",
     *   col("word").chainUDF("appendZUdf").chainUDF("prependAUdf")
     * )
     *
     * actualDf.show()
     * +-----+-------+
     * | word|    fun|
     * +-----+-------+
     * |dance|AdanceZ|
     * | sing| AsingZ|
     * +-----+-------+
     * }}}
     *
     * @group expr_ops
     */
    def chainUDF(udfName: String, cols: Column*): Column = {
      callUDF(
        udfName,
        col +: cols: _*
      )
    }

    /**
     * Like between, but geq when upper bound is null and leq when lower bound is null
     * The built in `between` doesn't work well when one of the bounds is undefined.  `nullBetween` is more useful when you have "less than or equal to" or "greater than or equal to" logic embedded in your upper and lower bounds.  For example, if the lower bound is `null` and the upper bound is `15`, `nullBetween` will interpret that as "all values below 15".
     *
     * Let's compare the `between` and `nullBetween` methods with a code snipped and the outputted DataFrame.
     *
     * {{{
     * val actualDF = sourceDF.withColumn(
     *   "between",
     *   col("age").between(col("lower_bound"), col("upper_bound"))
     * ).withColumn(
     *   "nullBetween",
     *   col("age").nullBetween(col("lower_bound"), col("upper_bound"))
     * )
     * +-----------+-----------+---+-------+-----------+
     * |lower_bound|upper_bound|age|between|nullBetween|
     * +-----------+-----------+---+-------+-----------+
     * |         10|         15| 11|   true|       true|
     * |         17|       null| 94|   null|       true|
     * |       null|         10|  5|   null|       true|
     * +-----------+-----------+---+-------+-----------+
     * }}}
     *
     * @group expr_ops
     */
    def nullBetween(lowerCol: Column, upperCol: Column): Column = {
      when(
        lowerCol.isNull && upperCol.isNull,
        false
      ).otherwise(
        when(
          col.isNull,
          false
        ).otherwise(
          when(
            lowerCol.isNull && upperCol.isNotNull && col.leq(upperCol),
            true
          ).otherwise(
            when(
              lowerCol.isNotNull && upperCol.isNull && col.geq(lowerCol),
              true
            ).otherwise(
              col.between(
                lowerCol,
                upperCol
              )
            )
          )
        )
      )
    }

    /**
     * Returns true if the current expression is true
     * Returns false if the current expression is null
     *
     * @group expr_ops
     */
    def isTrue: Column = {
      when(
        col.isNull,
        false
      ).otherwise(col === true)
    }

    /**
     * Returns true if the col is false
     * Returns false if the current expression is false
     *
     * @group expr_ops
     */
    def isFalse: Column = {
      when(
        col.isNull,
        false
      ).otherwise(col === false)
    }

    /**
     * Returns true if the col is false or null
     * `isFalsy` returns `true` if a column is `null` or `false` and `false` otherwise.
     *
     * Suppose you start with the following `sourceDF`:
     *
     * {{{
     * +------+
     * |is_fun|
     * +------+
     * |  true|
     * | false|
     * |  null|
     * +------+
     * }}}
     *
     * Run the `isFalsy` method:
     *
     * {{{
     * val actualDF = sourceDF.withColumn("is_fun_falsy", col("is_fun").isFalsy)
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * {{{
     * +------+------------+
     * |is_fun|is_fun_falsy|
     * +------+------------+
     * |  true|       false|
     * | false|        true|
     * |  null|        true|
     * +------+------------+
     * }}}
     *
     * @group expr_ops
     */
    def isFalsy: Column = {
      when(
        col.isNull || col === lit(false),
        true
      ).otherwise(false)
    }

    /**
     * Returns true if the col is not false or null
     * `isTruthy` returns `false` if a column is `null` or `false` and `true` otherwise.
     *
     * Suppose you start with the following `sourceDF`:
     *
     * {{{
     * +------+
     * |is_fun|
     * +------+
     * |  true|
     * | false|
     * |  null|
     * +------+
     * }}}
     *
     * Run the `isTruthy` method:
     *
     * {{{
     * val actualDF = sourceDF.withColumn("is_fun_truthy", col("is_fun").isTruthy)
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * {{{
     * +------+-------------+
     * |is_fun|is_fun_truthy|
     * +------+-------------+
     * |  true|         true|
     * | false|        false|
     * |  null|        false|
     * +------+-------------+
     * }}}
     *
     * @group expr_ops
     */
    def isTruthy: Column = {
      !col.isFalsy
    }

    /**
     * Returns true if the col is null or a blank string
     * The `isNullOrBlank` method returns `true` if a column is `null` or `blank` and `false` otherwise.
     *
     * Suppose you start with the following `sourceDF`:
     *
     * {{{
     * +-----------+
     * |animal_type|
     * +-----------+
     * |        dog|
     * |       null|
     * |         ""|
     * |     "    "|
     * +-----------+
     * }}}
     *
     * Run the `isNullOrBlank` method:
     *
     * {{{
     * val actualDF = sourceDF.withColumn(
     * "animal_type_is_null_or_blank",
     * col("animal_type").isNullOrBlank
     * )
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * {{{
     * +-----------+----------------------------+
     * |animal_type|animal_type_is_null_or_blank|
     * +-----------+----------------------------+
     * |        dog|                       false|
     * |       null|                        true|
     * |         ""|                        true|
     * |     "    "|                        true|
     * +-----------+----------------------------+
     * }}}
     *
     * @group expr_ops
     */
    def isNullOrBlank: Column = {
      col.isNull || trim(col) === ""
    }

    /**
     * Returns true if the col is not null or a blank string
     *
     * The `isNotNullOrBlank` method returns `true` if a column is not `null` or `blank` and `false` otherwise.
     * Suppose you start with the following `sourceDF`:
     *
     * +-------------+
     * |employee_name|
     * +-------------+
     * |         John|
     * |         null|
     * |           ""|
     * |       "    "|
     * +-------------+
     *
     * Run the `isNotNullOrBlank` method:
     *
     * {{{
     * val actualDF = sourceDF.withColumn(
     *   "employee_name_is_not_null_or_blank",
     *   col("employee_name").isNotNullOrBlank
     * )
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * +-------------+----------------------------------+
     * |employee_name|employee_name_is_not_null_or_blank|
     * +-------------+----------------------------------+
     * |         John|                              true|
     * |         null|                             false|
     * |           ""|                             false|
     * |       "    "|                             false|
     * +-------------+----------------------------------+
     *
     * @group expr_ops
     */
    def isNotNullOrBlank: Column = {
      !col.isNullOrBlank
    }

    /**
     * Returns true if the col is not in a list of elements
     * The `isNotIn` method returns `true` if a column element is not in a list and `false` otherwise.  It's the opposite of the `isin` method.
     *
     * Suppose you start with the following `sourceDF`:
     *
     * {{{
     * +-----+
     * |stuff|
     * +-----+
     * |  dog|
     * |shoes|
     * |laces|
     * | null|
     * +-----+
     * }}}
     *
     * Run the `isNotIn` method:
     *
     * {{{
     * val footwearRelated = Seq("laces", "shoes")
     *
     * val actualDF = sourceDF.withColumn(
     *   "is_not_footwear_related",
     *   col("stuff").isNotIn(footwearRelated: _*)
     * )
     * }}}
     *
     * Here are the contents of `actualDF`:
     *
     * {{{
     * +-----+-----------------------+
     * |stuff|is_not_footwear_related|
     * +-----+-----------------------+
     * |  dog|                   true|
     * |shoes|                  false|
     * |laces|                  false|
     * | null|                   null|
     * +-----+-----------------------+
     * }}}
     *
     * @group expr_ops
     */
    def isNotIn(list: Any*): Column = {
      not(col.isin(list: _*))
    }

    def evalString(): String = {
      col.expr.eval().toString
    }

  }

}
