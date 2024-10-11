package org.apache.spark.sql.daria

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.daria.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.stddev
import utest._

object functionsTests extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {
    'rand_gamma - {
      "has correct mean and standard deviation" - {
        val sourceDF = spark.range(100000).select(randGamma(2.0, 2.0))
        val stats = sourceDF
          .agg(
            mean("gamma_random").as("mean"),
            stddev("gamma_random").as("stddev")
          )
          .collect()(0)

        val gammaMean   = stats.getAs[Double]("mean")
        val gammaStddev = stats.getAs[Double]("stddev")

        // Gamma distribution with shape=2.0 and scale=2.0 has mean=4.0 and stddev=sqrt(8.0)
        assert(gammaMean > 0)
        assert(math.abs(gammaMean - 4.0) < 0.5)
        assert(math.abs(gammaStddev - math.sqrt(8.0)) < 0.5)
      }

      "has correct mean and standard deviation from shape/scale column" - {
        val sourceDF = spark
          .range(100000)
          .withColumn("shape", lit(2.0))
          .withColumn("scale", lit(2.0))
          .select(randGamma(col("shape"), col("shape")))
        val stats = sourceDF
          .agg(
            mean("gamma_random").as("mean"),
            stddev("gamma_random").as("stddev")
          )
          .collect()(0)

        val gammaMean   = stats.getAs[Double]("mean")
        val gammaStddev = stats.getAs[Double]("stddev")

        // Gamma distribution with shape=2.0 and scale=2.0 has mean=4.0 and stddev=sqrt(8.0)
        assert(gammaMean > 0)
        assert(math.abs(gammaMean - 4.0) < 0.5)
        assert(math.abs(gammaStddev - math.sqrt(8.0)) < 0.5)
      }
    }

    'rand_laplace - {
      "has correct mean and standard deviation" - {
        val sourceDF = spark.range(100000).select(randLaplace())
        val stats = sourceDF
          .agg(
            mean("laplace_random").as("mean"),
            stddev("laplace_random").as("std_dev")
          )
          .collect()(0)

        val laplaceMean   = stats.getAs[Double]("mean")
        val laplaceStdDev = stats.getAs[Double]("std_dev")

        // Laplace distribution with mean=0.0 and scale=1.0 has mean=0.0 and stddev=sqrt(2.0)
        assert(math.abs(laplaceMean) <= 0.1)
        assert(math.abs(laplaceStdDev - math.sqrt(2.0)) < 0.5)
      }
    }
  }
}
