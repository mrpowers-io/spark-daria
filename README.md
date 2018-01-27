# spark-daria

Spark helper methods to maximize developer productivity.

[![Build Status](https://travis-ci.org/MrPowers/spark-daria.svg?branch=master)](https://travis-ci.org/MrPowers/spark-daria)

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/cf2c0624682b4487a3b3e5c8330f1fbe)](https://www.codacy.com/app/MrPowers/spark-daria?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MrPowers/spark-daria&amp;utm_campaign=Badge_Grade)

![typical daria](https://github.com/MrPowers/spark-daria/blob/master/daria.png)

## Setup

**Option 1: Maven**

Fetch the JAR file from Maven.

```scala
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "mrpowers" % "spark-daria" % "2.2.0_0.12.0"
```

**Option 2: Spark Packages**

1. Add the [sbt-spark-package plugin](https://github.com/databricks/sbt-spark-package) to your application.  The spark-daria releases are maintained in [Spark Packages](https://spark-packages.org/package/mrpowers/spark-daria).

2. Update your build.sbt file: `spDependencies += "mrpowers/spark-daria:2.2.0_0.12.0"`

**Option 3: JitPack**

Update your `build.sbt` file as follows.

```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v2.2.0_0.17.1"
```

**Accessing spark-daria versions for different Spark versions**

Message me if you need spark-daria to be compiled with a different Spark version and I'll help you out :wink:

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

## DataFrame Extensions

### `printSchemaInCodeFormat`

Spark has a `printSchema` method to print the schema of a DataFrame and a `schema` method that returns a `StructType` object.

The `Dataset#schema` method can be easily converted into working code for small DataFrames, but it can be a lot of manual work for DataFrames with a lot of columns.

The `printSchemaInCodeFormat` DataFrame extension prints the DataFrame schema as a valid `StructType` object.

Suppose you have the following `sourceDF`:

```
+--------+--------+---------+
|    team|   sport|goals_for|
+--------+--------+---------+
|    jets|football|       45|
|nacional|  soccer|       10|
+--------+--------+---------+
```

`sourceDF.printSchemaInCodeFormat()` will output the following rows in the console:

```scala
StructType(
  List(
    StructField("team", StringType, true),
    StructField("sport", StringType, true),
    StructField("goals_for", IntegerType, true)
  )
)
```

### `composeTransforms`

Uses function composition to run a list of DataFrame transformations.

```scala
def withGreeting()(df: DataFrame): DataFrame = {
  df.withColumn("greeting", lit("hello world"))
}

def withCat(name: String)(df: DataFrame): DataFrame = {
  df.withColumn("cats", lit(s"$name meow"))
}

val transforms = List(
  withGreeting()(_),
  withCat("sandy")(_)
)

sourceDF.composeTransforms(transforms)
```

### `reorderColumns`

Reorders the columns in a DataFrame.

```scala
val actualDF = sourceDF.reorderColumns(
  Seq("greeting", "team", "cats")
)
```

The `actualDF` will have the `greeting` column first, then the `team` column then the `cats` column.

### `containsColumn`

```scala
sourceDF.containsColumn("team")
```

Returns `true` if `sourceDF` contains a column named `"team"` and false otherwise.

### `trans`

Enables you to specify the columns that should be added / removed by a custom transformations and errors out if the columns the columns that are actually added / removed are different.

```scala
val actualDF = sourceDF
  .trans(
    CustomTransform(
      transform = ExampleTransforms.withGreeting(),
      addedColumns = Seq("greeting"),
      requiredColumns = Seq("something")
    )
  )
  .trans(
    CustomTransform(
      transform = ExampleTransforms.withCat("spanky"),
      addedColumns = Seq("cats")
    )
  )
  .trans(
    CustomTransform(
      transform = ExampleTransforms.dropWordCol(),
      removedColumns = Seq("word")
    )
  )
```

## DataFrame Helpers

### `twoColumnsToMap`

* N.B. This method uses `collect` and should only be called on small DataFrames.*

Converts two columns in a DataFrame to a Map.

Suppose we have the following `sourceDF`:

```
+-----------+---------+
|     island|fun_level|
+-----------+---------+
|    boracay|        7|
|long island|        9|
+-----------+---------+
```

Let's convert this DataFrame to a Map with `island` as the key and `fun_level` as the value.

```scala
val actual = DataFrameHelpers.twoColumnsToMap[String, Integer](
  sourceDF,
  "island",
  "fun_level"
)

println(actual)

// Map(
//   "boracay" -> 7,
//   "long island" -> 9
// )
```

### `columnToArray`

* N.B. This method uses `collect` and should only be called on small DataFrames.*

This function converts a column to an array of items.

Suppose we have the following `sourceDF`:

```
+---+
|num|
+---+
|  1|
|  2|
|  3|
+---+
```

Let's convert the `num` column to an Array of values.  Let's run the code and view the results.

```scala
val actual = DataFrameHelpers.columnToArray[Int](sourceDF, "num")

println(actual)

// Array(1, 2, 3)
```

### `toArrayOfMaps`

* N.B. This method uses `collect` and should only be called on small DataFrames.*

Converts a DataFrame to an array of Maps.

Suppose we have the following `sourceDF`:

```
+----------+-----------+---------+
|profession|some_number|pay_grade|
+----------+-----------+---------+
|    doctor|          4|     high|
|   dentist|         10|     high|
+----------+-----------+---------+
```

Run the code to convert this DataFrame into an array of Maps.

```scala
val actual = DataFrameHelpers.toArrayOfMaps(sourceDF)

println(actual)

Array(
  Map("profession" -> "doctor", "some_number" -> 4, "pay_grade" -> "high"),
  Map("profession" -> "dentist", "some_number" -> 10, "pay_grade" -> "high")
)
```

## :zap: sql.functions

Spark [has a ton of SQL functions](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) and spark-daria is meant to fill in any gaps.

### `singleSpace`

```scala
val actualDF = sourceDF.withColumn(
  "some_string_single_spaced",
  singleSpace(col("some_string"))
)
```

Replaces all multispaces with single spaces (e.g. changes `"this   has     some"` to `"this has some"`.

### `removeAllWhitespace`

```scala
val actualDF = sourceDF.withColumn(
  "some_string_without_whitespace",
  removeAllWhitespace(col("some_string"))
)
```

Removes all whitespace in a string (e.g. changes `"this   has     some"` to `"thishassome"`.

You can also use this function with a `colName` string argument.

```scala
val actualDF = sourceDF.withColumn(
  "some_string_without_whitespace",
  removeAllWhitespace(col("some_string"))
)
```

### `antiTrim`

```scala
val actualDF = sourceDF.withColumn(
  "some_string_anti_trimmed",
  antiTrim(col("some_string"))
)
```

Removes all inner whitespace, but doesn't delete leading or trailing whitespace (e.g. changes `"  this   has     some   "` to `"  thishassome   "`.

### `removeNonWordCharacters`

```scala
val actualDF = sourceDF.withColumn(
  "some_string_remove_non_word_chars",
  removeNonWordCharacters(col("some_string"))
)
```

Removes all non-word characters from a string, excluding whitespace (e.g. changes `"  ni!!ce  h^^air person  "` to `"  nice  hair person  "`).

### `exists`

Scala has an Array#exists function that works like this:

```scala
Array(1, 2, 5).exists(_ % 2 == 0) // true
```

Suppose we have the following sourceDF:

```
+---------+
|     nums|
+---------+
|[1, 4, 9]|
|[1, 3, 5]|
+---------+
```

We can use the spark-daria `exists` function to see if there are even numbers in the arrays in the `nums` column.

```scala
val actualDF = sourceDF.withColumn(
  "nums_has_even",
  exists[Int]((x: Int) => x % 2 == 0).apply(col("nums"))
)
```

```
actualDF.show()

+---------+-------------+
|     nums|nums_has_even|
+---------+-------------+
|[1, 4, 9]|         true|
|[1, 3, 5]|        false|
+---------+-------------+
```

### `forall`

Scala has an Array#forall function that works like this:

```scala
Array("catdog", "crazy cat").forall(_.contains("cat")) // true
```

Suppose we have the following sourceDF:

```
+------------+
|       words|
+------------+
|[snake, rat]|
|[cat, crazy]|
+------------+
```

We can use the spark-daria `forall` function to see if all the strings in an array contain the string `"cat"`.

```scala
val actualDF = sourceDF.withColumn(
  "all_words_begin_with_c",
  forall[String]((x: String) => x.startsWith("c")).apply(col("words"))
)
```

```
actualDF.show()

+------------+----------------------+
|       words|all_words_begin_with_c|
+------------+----------------------+
|[snake, rat]|                 false|
|[cat, crazy]|                  true|
+------------+----------------------+
```

### `multiEquals`

Returns `true` if multiple columns are equal to a value.

Suppose we have the following sourceDF:

```
+---+---+
| s1| s2|
+---+---+
|cat|cat|
|cat|dog|
|pig|pig|
+---+---+
```

We can use the `multiEquals` function to see if multiple columns are equal to `"cat"`.

```scala
val actualDF = sourceDF.withColumn(
  "are_s1_and_s2_cat",
  multiEquals[String]("cat", col("s1"), col("s2"))
)
```

```
actualDF.show()

+---+---+-----------------+
| s1| s2|are_s1_and_s2_cat|
+---+---+-----------------+
|cat|cat|             true|
|cat|dog|            false|
|pig|pig|            false|
+---+---+-----------------+
```

### `yeardiff`

There is a `datediff` function that calculates the number of days between two dates, but there isn't a `yeardiff` function that calculates the number of years between two dates.

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

### `truncate`

```scala
sourceDF.withColumn(
  "some_string_truncated",
  truncate(col("some_string"), 3)
)
```

Truncates the `"some_string"` column to only have three characters.

## :trident: sql.transformations

SQL transformations take a DataFrame as an argument and return a DataFrame.  They are suitable arguments for the `Dataset#transform` method.

It's convenient to work with DataFrames that have snake\_case column names.  Column names with spaces make it harder to write SQL queries.

### `sortColumns`

The `sortColumns` transformation sorts the columns in a DataFrame alphabetically.

Suppose you start with the following `sourceDF`:

```
+-----+---+-----+
| name|age|sport|
+-----+---+-----+
|pablo|  3| polo|
+-----+---+-----+
```

Run the code:

```
val actualDF = sourceDF.transform(sortColumns())
```

Here’s the `actualDF`:

```
+---+-----+-----+
|age| name|sport|
+---+-----+-----+
|  3|pablo| polo|
+---+-----+-----+
```

### `snakeCaseColumns`

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

### `multiRegexpReplace`

```scala
val actualDF = sourceDF.transform(
  transformations.multiRegexpReplace(
    List(col("person"), col("phone")),
    "cool",
    "dude"
  )
)
```

Replaces all `"cool"` strings in the `person` and `phone` columns with the string `"dude"`.

### `bulkRegexpReplace`

```scala
val actualDF = sourceDF.transform(
  transformations.bulkRegexpReplace(
    "cool",
    "dude"
  )
)
```

Replaces all `"cool"` strings in all the `sourceDF` columns of `StringType` with the string `"dude"`.

### `truncateColumns`

```scala
val columnLengths: Map[String, Int] = Map(
  "person" -> 2,
  "phone" -> 3
)

sourceDF.transform(
  truncateColumns(columnLengths)
)
```

Limits the `"person"` column to 2 characters and the `"phone"` column to 3 characters.

## ETL

spark-daria can be used as a lightweight framework for running ETL analyses in Spark.

You can define EtlDefinitions, group them in a collection, and run the etls via jobs.

### Components of an ETL

An ETL starts with a DataFrame, runs a series of transformations (filter, custom transformations, repartition), and writes out data.

The `EtlDefinition` class is generic and can be molded to suit all ETL situations.  For example, it can read a CSV file from S3, run transformations, and write out Parquet files on your local filesystem.

### Code example

This snippet creates a DataFrame and writes it out as a CSV file in your local filesystem.

```scala
val sourceDF = spark.createDF(
  List(
    ("bob", 14),
    ("liz", 20)
  ), List(
    ("name", StringType, true),
    ("age", IntegerType, true)
  )
)

def someTransform()(df: DataFrame): DataFrame = {
  df.withColumn("cool", lit("dude"))
}

def someWriter()(df: DataFrame): Unit = {
  val path = new java.io.File("./tmp/example").getCanonicalPath
  df.repartition(1).write.csv(path)
}

val etlDefinition = new EtlDefinition(
  name =  "example",
  sourceDF = sourceDF,
  transform = someTransform(),
  write = someWriter(),
  hidden = false
)

etlDefinition.process()
```

In production applications, it's more likely that you'll use Spark DataFrame readers to create the `sourceDF` (e.g. `spark.read.parquet("some_s3_path")`).

### Example production use case

You can define a collection of ETL definitions in a Databricks notebook and create a Slack command that runs an EtlDefinition on command from Slack.

## :two_women_holding_hands: :two_men_holding_hands: :couple: Contribution Criteria

We are actively looking for contributors to add functionality that fills in the gaps of the Spark source code.

To get started, fork the project and submit a pull request.  Please write tests!

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.

