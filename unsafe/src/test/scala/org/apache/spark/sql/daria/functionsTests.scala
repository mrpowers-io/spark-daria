package org.apache.spark.sql.daria

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import org.apache.spark.sql.daria.functions._
import org.apache.spark.sql.{functions => F}
import utest._

object functionsTests extends TestSuite with DataFrameComparer with ColumnComparer with SparkSessionTestWrapper {

  val tests = Tests {
    'rand_gamma - {
      "has correct mean and standard deviation" - {
        val sourceDF = spark.range(100000).select(rand_gamma(2.0, 2.0))
        val stats = sourceDF
          .agg(
            F.mean("gamma_random").as("mean"),
            F.stddev("gamma_random").as("stddev")
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
        val sourceDF = spark.range(100000).select(rand_laplace())
        val stats = sourceDF
          .agg(
            F.mean("laplace_random").as("mean"),
            F.stddev("laplace_random").as("std_dev")
          )
          .collect()(0)

        val laplaceMean   = stats.getAs[Double]("mean")
        val laplaceStdDev = stats.getAs[Double]("std_dev")

        // Laplace distribution with mean=0.0 and scale=1.0 has mean=0.0 and stddev=sqrt(2.0)
        assert(math.abs(laplaceMean) <= 0.1)
        assert(math.abs(laplaceStdDev - math.sqrt(2.0)) < 0.5)
      }
    }

    'rand - {
      "has correct min and max" - {
        val min      = 5
        val max      = 10
        val sourceDF = spark.range(100000).select(rand_range(min, max).as("rand_min_max"))
        val stats = sourceDF
          .agg(
            F.min("rand_min_max").as("min"),
            F.min("rand_min_max").as("max")
          )
          .collect()(0)

        val uniformMin = stats.getAs[Double]("min")
        val uniformMax = stats.getAs[Double]("max")

        assert(uniformMin >= min)
        assert(uniformMax <= max)
      }
    }

    'randn - {
      "has correct mean and variance" - {
        val mean     = 1
        val variance = 2
        val sourceDF = spark.range(100000).select(randn(mean, variance).as("rand_normal"))
        val stats = sourceDF
          .agg(
            F.mean("rand_normal").as("mean"),
            F.variance("rand_normal").as("variance")
          )
          .collect()(0)

        val normalMean     = stats.getAs[Double]("mean")
        val normalVariance = stats.getAs[Double]("variance")

        assert(math.abs(normalMean - mean) <= 0.1)
        assert(math.abs(normalVariance - variance) <= 0.1)
      }
    }
  }
}
