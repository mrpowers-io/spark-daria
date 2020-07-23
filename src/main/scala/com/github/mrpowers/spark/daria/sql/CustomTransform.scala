package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.DataFrame

case class CustomTransform(
    transform: (DataFrame => DataFrame),
    requiredColumns: Seq[String] = Seq.empty[String],
    addedColumns: Seq[String] = Seq.empty[String],
    removedColumns: Seq[String] = Seq.empty[String],
    skipWhenPossible: Boolean = true
)
