package com.github.mrpowers.spark.daria.sql

import utest._

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}

object EtlDefinitionTest extends TestSuite with SparkSessionTestWrapper with DataFrameComparer {

  val tests = Tests {

    'new - {

      "creates a new EtlDefinition object with a metadata property" - {

        val sourceDF = spark.createDF(
          List(
            ("bob", 14),
            ("liz", 20)
          ),
          List(
            ("name", StringType, true),
            ("age", IntegerType, true)
          )
        )

        val etlDefinition =
          new EtlDefinition(
            sourceDF = sourceDF,
            transform = EtlHelpers.someTransform(),
            write = EtlHelpers.someWriter(),
            metadata = scala.collection.mutable.Map("hidden" -> true)
          )

        assert(etlDefinition.metadata("hidden") == true)

      }

      "allows objects to be created without setting metadata" - {

        val sourceDF = spark.createDF(
          List(
            ("bob", 14),
            ("liz", 20)
          ),
          List(
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

    'process - {

      "runs a full ETL process and writes out data to a folder" - {

        val sourceDF = spark.createDF(
          List(
            ("bob", 14),
            ("liz", 20)
          ),
          List(
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

    'etlCollection - {

      "can run etls that are organized in a map" - {

        val sourceDF = spark.createDF(
          List(
            ("bob", 14),
            ("liz", 20)
          ),
          List(
            ("name", StringType, true),
            ("age", IntegerType, true)
          )
        )

        val etlDefinition = new EtlDefinition(
          sourceDF = sourceDF,
          transform = EtlHelpers.someTransform(),
          write = EtlHelpers.someWriter()
        )

        val etls = scala.collection.mutable
          .Map[String, EtlDefinition]("example" -> etlDefinition)

        etls += ("ex2" -> etlDefinition)

        etls("example").process()

      }

    }

  }

}
