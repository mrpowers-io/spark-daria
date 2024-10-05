package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

/**
 * spark-daria can be used as a lightweight framework for running ETL analyses in Spark.
 *
 * You can define `EtlDefinitions`, group them in a collection, and run the etls via jobs.
 *
 * '''Components of an ETL'''
 *
 * An ETL starts with a DataFrame, runs a series of transformations (filter, custom transformations, repartition), and writes out data.
 *
 * The `EtlDefinition` class is generic and can be molded to suit all ETL situations.  For example, it can read a CSV file from S3, run transformations, and write out Parquet files on your local filesystem.
 */
case class EtlDefinition(
    sourceDF: DataFrame,
    transform: (DataFrame => DataFrame),
    write: (DataFrame => Unit),
    metadata: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
) {

  /**
   * Runs an ETL process
   *
   * {{{
   * val sourceDF = spark.createDF(
   * List(
   *   ("bob", 14),
   *   ("liz", 20)
   *  ), List(
   *   ("name", StringType, true),
   *   ("age", IntegerType, true)
   *  )
   * )
   *
   * def someTransform()(df: DataFrame): DataFrame = {
   *   df.withColumn("cool", lit("dude"))
   * }
   *
   * def someWriter()(df: DataFrame): Unit = {
   *   val path = new java.io.File("./tmp/example").getCanonicalPath
   *   df.repartition(1).write.csv(path)
   * }
   *
   * val etlDefinition = new EtlDefinition(
   *   name =  "example",
   *   sourceDF = sourceDF,
   *   transform = someTransform(),
   *   write = someWriter(),
   *   hidden = false
   * )
   *
   * etlDefinition.process()
   * }}}
   */
  def process(): Unit = {
    write(sourceDF.transform(transform))
  }

}
