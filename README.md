# spark-daria

Open source Spark functions and transformations.

![typical daria](https://github.com/MrPowers/spark-daria/blob/master/daria.png)

## Representative Examples

The [https://github.com/MrPowers/spark-daria/wiki](https://github.com/MrPowers/spark-daria/wiki) contains documentation for the public methods provided by Daria.

The README contains a subset of examples to familiarize new visitors with the project.

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

val actualDf = sourceDf.transform(transformations.snakeCaseColumns)

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

