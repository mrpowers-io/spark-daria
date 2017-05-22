# spark-daria

Spark helper methods to maximize efficiency and developer happiness.

[![Build Status](https://travis-ci.org/MrPowers/spark-daria.svg?branch=master)](https://travis-ci.org/MrPowers/spark-daria)

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/cf2c0624682b4487a3b3e5c8330f1fbe)](https://www.codacy.com/app/MrPowers/spark-daria?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MrPowers/spark-daria&amp;utm_campaign=Badge_Grade)

![typical daria](https://github.com/MrPowers/spark-daria/blob/master/daria.png)

## :white_check_mark: DataFrameValidator

Custom transformations often make assumptions about the presence or absence of columns in the DataFrame. It's important to document these dependencies in the code and provide users with descriptive error messages if assumptions are not met.

Here's an example of a custom transformation that uses the validatePresenceOfColumns validation.

```scala
import com.github.mrpowers.spark.daria.sql.DataFrameValidator

object MyTransformations extends DataFrameValidator {

  def withStandardizedPersonInfo(df: DataFrame): DataFrame = {
    val requiredColNames = Seq("name", "age")
    validatePresenceOfColumns(df, requiredColNames)
    // some transformation code
  }

}
```

### `validatePresenceOfColumns()`

Validates if columns are included in a DataFrame. This code will error out:

```scala
val sourceDF = Seq(
  ("jets", "football"),
  ("nacional", "soccer")
).toDF("team", "sport")

val requiredColNames = Seq("team", "sport", "country", "city")

validatePresenceOfColumns(sourceDF, requiredColNames)
```

This is the error message:

> com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException: The [country, city] columns are not included in the DataFrame with the following columns [team, sport]


### `validateSchema()`

Validates the schema of a DataFrame. This code will error out:

```scala
val sourceData = List(
  Row(1, 1),
  Row(-8, 8),
  Row(-5, 5),
  Row(null, null)
)

val sourceSchema = List(
  StructField("num1", IntegerType, true),
  StructField("num2", IntegerType, true)
)

val sourceDF = spark.createDataFrame(
  spark.sparkContext.parallelize(sourceData),
  StructType(sourceSchema)
)

val requiredSchema = StructType(
  List(
    StructField("num1", IntegerType, true),
    StructField("num2", IntegerType, true),
    StructField("name", StringType, true)
  )
)

validateSchema(sourceDF, requiredSchema)
```

This is the error message:

> com.github.mrpowers.spark.daria.sql.InvalidDataFrameSchemaException: The [StructField(name,StringType,true)] StructFields are not included in the DataFrame with the following StructFields [StructType(StructField(num1,IntegerType,true), StructField(num2,IntegerType,true))]

### `validateAbsenceOfColumns()`

Validates columns are not included in a DataFrame. This code will error out:

```scala
val sourceDF = Seq(
  ("jets", "football"),
  ("nacional", "soccer")
).toDF("team", "sport")

val prohibitedColNames = Seq("team", "sport", "country", "city")

validateAbsenceOfColumns(sourceDF, prohibitedColNames)
```

This is the error message:

> com.github.mrpowers.spark.daria.sql.ProhibitedDataFrameColumnsException: The [team, sport] columns are not allowed to be included in the DataFrame with the following columns [team, sport]

## :heart_eyes: Creating DataFrames

Spark provides two methods for creating DataFrames:

* `createDataFrame` is verbose
* `toDF` doesn't provide enough control for customizing the schema

spark-daria defined a createDF method that allows for the terse syntax of `toDF` and the control of `createDataFrame`.

```scala
spark.createDF(
  List(
    ("bob", 45),
    ("liz", 25),
    ("freeman", 32)
  ), List(
    ("name", StringType, true),
    ("age", IntegerType, false)
  )
)
```

The `createDF` method can also be used with lists of `Row` and `StructField` objects.

```scala
spark.createDF(
  List(
    Row("bob", 45),
    Row("liz", 25),
    Row("freeman", 32)
  ), List(
    StructField("name", StringType, true),
    StructField("age", IntegerType, false)
  )
)
```

## :link: Chaining UDFs and SQL functions

The ColumnExt class monkey patches the org.apache.spark.sql.Column class, so SQL functions and user defined functions can be chained ([relevant blog post](https://medium.com/@mrpowers/chaining-spark-sql-functions-and-user-defined-functions-2e98534b6885)).

### `chain()`

The chain method takes a org.apache.spark.sql.functions function as an argument and can be used as follows:

```scala
val wordsDf = Seq(
  ("Batman  "),
  ("  CATWOMAN"),
  (" pikachu ")
).toDF("word")

val actualDf = wordsDf.withColumn(
  "cleaned_word",
  col("word").chain(lower).chain(trim)
)

actualDf.show()
```

```
+----------+------------+
|      word|cleaned_word|
+----------+------------+
|  Batman  |      batman|
|  CATWOMAN|    catwoman|
|  pikachu |     pikachu|
+----------+------------+
```

### `chainUDF()`

The chainUDF method takes the name of a user defined function as an argument and can be used as follows:

```scala
def appendZ(s: String): String = {
  s"${s}Z"
}

spark.udf.register("appendZUdf", appendZ _)

def prependA(s: String): String = {
  s"A${s}"
}

spark.udf.register("prependAUdf", prependA _)

val hobbiesDf = Seq(
  ("dance"),
  ("sing")
).toDF("word")

val actualDf = hobbiesDf.withColumn(
  "fun",
  col("word").chainUDF("appendZUdf").chainUDF("prependAUdf")
)

actualDf.show()
```

```
+-----+-------+
| word|    fun|
+-----+-------+
|dance|AdanceZ|
| sing| AsingZ|
+-----+-------+
```

### Using `chain()` and `chainUDF()` together

The chain and chainUDF methods can be used together as follows:

```scala
def appendZ(s: String): String = {
  s"${s}Z"
}

spark.udf.register("appendZUdf", appendZ _)

val wordsDf = Seq(
  ("Batman  "),
  ("  CATWOMAN"),
  (" pikachu ")
).toDF("word")

val actualDf = wordsDf.withColumn(
  "cleaned_word",
  col("word").chain(lower).chain(trim).chainUDF("appendZUdf")
)

actualDf.show()
```

```
+----------+------------+
|      word|cleaned_word|
+----------+------------+
|  Batman  |     batmanZ|
|  CATWOMAN|   catwomanZ|
|  pikachu |    pikachuZ|
+----------+------------+
```

## :zap: sql.functions

Spark [has a ton of SQL functions](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) and spark-daria is meant to fill in any gaps.

For example, there is a `datediff` function that calculates the number of days between two dates, but there isn't a `yeardiff` function that calculates the number of years between two dates.

The `com.github.mrpowers.spark.daria.sql.functions.yeardiff` function fills the gap.  Let's see how it works!

Suppose we have the following `testDf`

```
+--------------------+--------------------+
|      first_datetime|     second_datetime|
+--------------------+--------------------+
|2016-09-10 00:00:...|2001-08-10 00:00:...|
|2016-04-18 00:00:...|2010-05-18 00:00:...|
|2016-01-10 00:00:...|2013-08-10 00:00:...|
|                null|                null|
+--------------------+--------------------+
```

We can run the `yeardiff` function as follows:

```scala
import com.github.mrpowers.spark.daria.sql.functions._

val actualDf = testDf
  .withColumn("num_years", yeardiff(col("first_datetime"), col("second_datetime")))

actualDf.show()
```

Console output:

```
+--------------------+--------------------+------------------+
|      first_datetime|     second_datetime|         num_years|
+--------------------+--------------------+------------------+
|2016-09-10 00:00:...|2001-08-10 00:00:...|15.095890410958905|
|2016-04-18 00:00:...|2010-05-18 00:00:...| 5.923287671232877|
|2016-01-10 00:00:...|2013-08-10 00:00:...| 2.419178082191781|
|                null|                null|              null|
+--------------------+--------------------+------------------+
```

## :trident: sql.transformations

SQL transformations take a DataFrame as an argument and return a DataFrame.  They are suitable arguments for the `Dataset#transform` method.

It's convenient to work with DataFrames that have snake\_case column names.  Column names with spaces make it harder to write SQL queries.

spark-daria defines a `com.github.mrpowers.spark.daria.sql.transformations.snakeCaseColumns` transformation to convert all the column names to snake\_case.

```scala
import com.github.mrpowers.spark.daria.sql.transformations._

val sourceDf = Seq(
  ("funny", "joke")
).toDF("A b C", "de F")

val actualDf = sourceDf.transform(snakeCaseColumns)

actualDf.show()
```

Console output:

```
+-----+----+
|a_b_c|de_f|
+-----+----+
|funny|joke|
+-----+----+
```

## :two_women_holding_hands: :two_men_holding_hands: :couple: Contribution Criteria

We are actively looking for contributors to add functionality that fills in the gaps of the Spark source code.

To get started, fork the project and submit a pull request.  Please write tests!

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.

