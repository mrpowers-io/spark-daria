package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}

class EtlDefinitionSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  describe("new") {

    it("creates a new EtlDefinition object with a metadata property") {

      val sourceDF = spark.createDF(
        List(
          ("bob", 14),
          ("liz", 20)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val etlDefinition = new EtlDefinition(
        sourceDF = sourceDF,
        transform = EtlHelpers.someTransform(),
        write = EtlHelpers.someWriter(),
        metadata = scala.collection.mutable.Map("hidden" -> true)
      )

      assert(etlDefinition.metadata("hidden") === true)

    }

    it("allows objects to be created without setting metadata") {

      val sourceDF = spark.createDF(
        List(
          ("bob", 14),
          ("liz", 20)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val etlDefinition = new EtlDefinition(
        sourceDF = sourceDF,
        transform = EtlHelpers.someTransform(),
        write = EtlHelpers.someWriter()
      )

    }

  }

  describe("process") {

    it("runs a full ETL process and writes out data to a folder") {

      val sourceDF = spark.createDF(
        List(
          ("bob", 14),
          ("liz", 20)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val etlDefinition = new EtlDefinition(
        sourceDF = sourceDF,
        transform = EtlHelpers.someTransform(),
        write = EtlHelpers.someWriter()
      )

      etlDefinition.process()

    }

  }

  describe("etl collection") {

    it("can run etls that are organized in a map") {

      val sourceDF = spark.createDF(
        List(
          ("bob", 14),
          ("liz", 20)
        ), List(
          ("name", StringType, true),
          ("age", IntegerType, true)
        )
      )

      val etlDefinition = new EtlDefinition(
        sourceDF = sourceDF,
        transform = EtlHelpers.someTransform(),
        write = EtlHelpers.someWriter()
      )

      val etls = scala.collection.mutable.Map[String, EtlDefinition]("example" -> etlDefinition)

      etls += ("ex2" -> etlDefinition)

      etls("example").process()

    }

  }

}
