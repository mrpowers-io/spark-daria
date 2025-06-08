package org.apache.spark.sql.daria

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer, SchemaComparer}
import DataFrameExt.DataFrameMethods
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import utest._

object DataFrameExtTests extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {
    'toSchemaWithNullabilityAligned - {
      "align from nullable to non nullable from of nested schema" - {

        val data = Seq(
          Row(
            Row(
              "bayVal",
              "baxVal",
              Row("this", "yoVal"),
              "is"
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bay",
                    StringType,
                  ),
                  StructField(
                    "bax",
                    StringType,
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                        ),
                        StructField(
                          "yoo",
                          StringType,
                        )
                      )
                    )
                  ),
                  StructField(
                    "baz",
                    StringType,
                  )
                )
              ),
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType))), containsNull = false), containsNull = false),
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
            ),
            StructField(
              "x",
              StringType,
            ),
            StructField(
              "y",
              StringType,
            ),
            StructField(
              "z",
              StringType,
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "yoo",
                          StringType,
                          nullable = false
                        ),
                        StructField(
                          "zoo",
                          StringType,
                          nullable = false

                        )
                      )
                    ),
                    nullable = false
                  ),
                  StructField(
                    "bax",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bay",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "baz",
                    StringType,
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))), containsNull = false), containsNull = false),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))), false),
              nullable = false
            ),
            StructField(
              "x",
              StringType,
              nullable = false
            ),
            StructField(
              "y",
              StringType,
              nullable = false
            ),
            StructField(
              "z",
              StringType,
              nullable = false
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          ).toSchemaWithNullabilityAligned(expectedSchema)

        val expectedData = Seq(
          Row(
            Row(
              Row("yoVal", "this"),
              "baxVal",
              "bayVal",
              "is"
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from non nullable to nullable from of nested schema" - {
        val data = Seq(
          Row(
            Row(
              "bayVal",
              "baxVal",
              Row("this", "yoVal"),
              "is"
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bay",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bax",
                    StringType,
                    nullable = false
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                        ),
                        StructField(
                          "yoo",
                          StringType,
                        )
                      )
                    ),
                    nullable = false
                  ),
                  StructField(
                    "baz",
                    StringType,
                    nullable = false
                  )
                )
              ),
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType, nullable = false), StructField("v1", StringType, nullable = false))))),
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
              nullable = false
            ),
            StructField(
              "x",
              StringType,
              nullable = false
            ),
            StructField(
              "y",
              StringType,
              nullable = false
            ),
            StructField(
              "z",
              StringType,
              nullable = false
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "foo",
              StructType(
                Seq(
                  StructField(
                    "bay",
                    StringType,
                  ),
                  StructField(
                    "bax",
                    StringType,
                  ),
                  StructField(
                    "bar",
                    StructType(
                      Seq(
                        StructField(
                          "zoo",
                          StringType,
                        ),
                        StructField(
                          "yoo",
                          StringType,
                        )
                      )
                    )
                  ),
                  StructField(
                    "baz",
                    StringType,
                  )
                )
              ),
              nullable = true
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType))))),
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
            ),
            StructField(
              "x",
              StringType,
            ),
            StructField(
              "y",
              StringType,
            ),
            StructField(
              "z",
              StringType,
            )
          )
        )

        val inputDf =  spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          )
        inputDf.printSchema()
        inputDf.show(false)

        val df = inputDf.toSchemaWithNullabilityAligned(expectedSchema)

        val expectedData = Seq(
          Row(
            Row(
              Row("yoVal", "this"),
              "baxVal",
              "bayVal",
              "is"
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            "something",
            "cool",
            ";)"
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from nullable to non nullable for nested array of struct" - {
        val data = Seq(
          Row(
            Row(
              Seq(
                Row("a4Val", "a2Val")
              )
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x4Val", "x3Val")
                )
              )
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(StructType(Seq(StructField("a4", StringType), StructField("a2", StringType))), containsNull = false),
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType))), containsNull = false), containsNull = false),
              nullable = true
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
              nullable = true
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(StructField("x1", ArrayType(StructType(Seq(StructField("b", StringType), StructField("a", StringType))), containsNull = false), nullable = true))
                ),
                containsNull = false
              ),
              nullable = true
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(StructType(Seq(StructField("a2", StringType, nullable = false), StructField("a4", StringType, nullable = false))), containsNull = false),
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))), containsNull = false), containsNull = false),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))), false),
              nullable = false
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(
                    StructField("x1", ArrayType(StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", StringType, nullable = false))), containsNull = false), nullable = false)
                  )
                ),
                containsNull = false
              ),
              nullable = false
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          ).toSchemaWithNullabilityAligned(expectedSchema)

        val expectedData = Seq(
          Row(
            Row(
              Seq(
                Row("a2Val", "a4Val")
              )
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x3Val", "x4Val")
                )
              )
            )
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }

      "align from non nullable to nullable for nested array of struct" - {
        val data = Seq(
          Row(
            Row(
              Seq(
                Row("a4Val", "a2Val")
              )
            ),
            Seq(
              Seq(Row("yVal", "xVal"), Row("yVal1", "xVal1"))
            ),
            Seq(
              Row("yVal", "xVal"),
              Row("yVal1", "xVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x4Val", "x3Val")
                )
              )
            )
          )
        )

        val schema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(StructType(Seq(StructField("a2", StringType, nullable = false), StructField("a4", StringType, nullable = false))), containsNull = false),
                    nullable = false
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v1", StringType, nullable = false), StructField("v2", StringType, nullable = false))), containsNull = false), containsNull = false),
              nullable = false
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("x", StringType, nullable = false), StructField("y", StringType, nullable = false))), false),
              nullable = false
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(
                    StructField("x1", ArrayType(StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", StringType, nullable = false))), containsNull = false), nullable = false)
                  )
                ),
                containsNull = false
              ),
              nullable = false
            )
          )
        )

        val expectedSchema = StructType(
          Seq(
            StructField(
              "a1",
              StructType(
                Seq(
                  StructField(
                    "a2",
                    ArrayType(StructType(Seq(StructField("a4", StringType), StructField("a2", StringType)))),
                  )
                )
              ),
              nullable = false
            ),
            StructField(
              "v",
              ArrayType(ArrayType(StructType(Seq(StructField("v2", StringType), StructField("v1", StringType))))),
              nullable = true
            ),
            StructField(
              "w",
              ArrayType(StructType(Seq(StructField("y", StringType), StructField("x", StringType)))),
              nullable = true
            ),
            StructField(
              "x",
              ArrayType(
                StructType(
                  Seq(StructField("x1", ArrayType(StructType(Seq(StructField("b", StringType), StructField("a", StringType)))), nullable = true))
                ),
              ),
              nullable = true
            )
          )
        )

        val df = spark
          .createDataFrame(
            spark.sparkContext.parallelize(data),
            StructType(schema)
          ).toSchemaWithNullabilityAligned(expectedSchema)

        val expectedData = Seq(
          Row(
            Row(
              Seq(
                Row("a2Val", "a4Val")
              )
            ),
            Seq(
              Seq(
                Row("xVal", "yVal"),
                Row("xVal1", "yVal1")
              )
            ),
            Seq(
              Row("xVal", "yVal"),
              Row("xVal1", "yVal1")
            ),
            Seq(
              Row(
                Seq(
                  Row("x3Val", "x4Val")
                )
              )
            )
          )
        )

        val expectedDF = spark
          .createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
          )

        assertSmallDataFrameEquality(
          df,
          expectedDF,
          ignoreNullable = false
        )
      }
    }
  }
}
