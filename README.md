# spark-daria

Open source Spark functions and transformations.

![typical daria](https://github.com/MrPowers/spark-daria/blob/master/daria.png)

## Representative Examples

The [wiki](https://github.com/MrPowers/spark-daria/wiki) contains documentation for the public methods provided by Daria.

This README contains a subset of examples to familiarize new visitors with the project.

### DataFrameValidator

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

### SQL Column Extensions

The ColumnExt class monkey patches the org.apache.spark.sql.Column class, so SQL functions and user defined functions can be chained ([relevant blog post](https://medium.com/@mrpowers/chaining-spark-sql-functions-and-user-defined-functions-2e98534b6885)).

### sql.functions

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


### sql.transformations

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

## Contribution Criteria

We are actively looking for contributors to add functionality that fills in the gaps of the Spark source code.

To get started, fork the project and submit a pull request.  Please write tests!

After submitting a couple of good pull requests, you'll be added as a contributor to the project.

Continued excellence will be rewarded with push access to the master branch.

