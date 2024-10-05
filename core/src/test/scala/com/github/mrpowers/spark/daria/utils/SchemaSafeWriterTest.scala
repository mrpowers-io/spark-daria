package com.github.mrpowers.spark.daria.utils

import com.github.mrpowers.spark.daria.sql.SparkSessionTestWrapper
import utest._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types._

object SchemaSafeWriterTest extends TestSuite with SparkSessionTestWrapper {

  val tests = Tests {

    "parquetAppend" - {

      "works when new schema aligns with existing schema" - {
        val path = os.pwd / "tmp" / "people_one"
        if (os.exists(path)) os.remove.all(path)
        val df1 = spark.createDF(
          List(
            ("alice", 2),
            ("fred", 42)
          ),
          List(
            ("first_name", StringType, true),
            ("age", IntegerType, true)
          )
        )
        df1.write.parquet(path.toString())
        val df2 = spark.createDF(
          List(
            ("tiger", 43),
            ("maria", 45)
          ),
          List(
            ("first_name", StringType, true),
            ("age", IntegerType, true)
          )
        )
        SchemaSafeWriter.parquetAppend(path.toString(), df2)
        if (os.exists(path)) os.remove.all(path)
      }

      "errors out when the new schema doesn't match" - {
        val path = os.pwd / "tmp" / "people_one"
        if (os.exists(path)) os.remove.all(path)
        val df1 = spark.createDF(
          List(
            ("alice", 2),
            ("fred", 42)
          ),
          List(
            ("first_name", StringType, true),
            ("age", IntegerType, true)
          )
        )
        df1.write.parquet(path.toString())
        val df2 = spark.createDF(
          List(
            ("tiger", 43, "hi"),
            ("maria", 45, "hi")
          ),
          List(
            ("first_name", StringType, true),
            ("age", IntegerType, true),
            ("hello", StringType, true)
          )
        )
        val e = intercept[DariaSchemaMismatchError] {
          SchemaSafeWriter.parquetAppend(path.toString(), df2)
        }
        if (os.exists(path)) os.remove.all(path)
      }

    }

//    "deltaAppend" - {
//
//      "works when new schema aligns with existing schema" - {
//        val path = os.pwd / "tmp" / "people_two"
//        if (os.exists(path)) os.remove.all(path)
//        val df1 = spark.createDF(
//          List(
//            ("alice", 2),
//            ("fred", 42)
//          ),
//          List(
//            ("first_name", StringType, true),
//            ("age", IntegerType, true)
//          )
//        )
//        df1.write.format("delta").save(path.toString())
//        val df2 = spark.createDF(
//          List(
//            ("tiger", 43),
//            ("maria", 45)
//          ),
//          List(
//            ("first_name", StringType, true),
//            ("age", IntegerType, true)
//          )
//        )
//        SchemaSafeWriter.deltaAppend(path.toString(), df2)
//        if (os.exists(path)) os.remove.all(path)
//      }
//
//      "errors out when the new schema doesn't match" - {
//        val path = os.pwd / "tmp" / "people_two"
//        if (os.exists(path)) os.remove.all(path)
//        val df1 = spark.createDF(
//          List(
//            ("alice", 2),
//            ("fred", 42)
//          ),
//          List(
//            ("first_name", StringType, true),
//            ("age", IntegerType, true)
//          )
//        )
//        df1.write.format("delta").save(path.toString())
//        val df2 = spark.createDF(
//          List(
//            ("tiger", 43, "hi"),
//            ("maria", 45, "hi")
//          ),
//          List(
//            ("first_name", StringType, true),
//            ("age", IntegerType, true),
//            ("hello", StringType, true)
//          )
//        )
//        val e = intercept[DariaSchemaMismatchError] {
//          SchemaSafeWriter.deltaAppend(path.toString(), df2)
//        }
//        if (os.exists(path)) os.remove.all(path)
//      }
//
//    }

  }

}
