package com.github.mrpowers.spark.daria.sql

import org.scalatest.FunSpec
import SparkSessionExt._
import org.apache.spark.sql.types.{IntegerType, StringType}
import DataFrameExt._

class DataFrameExtSpec
    extends FunSpec
    with SparkSessionTestWrapper {

  describe("#printSchemaInCodeFormat") {

    it("prints the schema in a code friendly format") {

      val sourceDF = spark.createDF(
        List(
          ("jets", "football", 45),
          ("nacional", "soccer", 10)
        ), List(
          ("team", StringType, true),
          ("sport", StringType, true),
          ("goals_for", IntegerType, true)
        )
      )

//      uncomment the next line if you want to check out the console output
//      sourceDF.printSchemaInCodeFormat()

    }

  }

}
