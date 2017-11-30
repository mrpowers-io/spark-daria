package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class CustomTransform(
  transform: (DataFrame => DataFrame),
  columnsAdded: Seq[String] = Seq.empty[String],
  columnsRemoved: Seq[String] = Seq.empty[String]
)
