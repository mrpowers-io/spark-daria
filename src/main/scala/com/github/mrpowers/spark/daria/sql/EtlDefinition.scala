package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class EtlDefinition(
    sourceDF: DataFrame,
    transform: (DataFrame => DataFrame),
    write: (DataFrame => Unit),
    metadata: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
) {

  def process(): Unit = {
    write(sourceDF.transform(transform))
  }

}

