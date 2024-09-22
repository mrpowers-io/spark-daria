package org.apache.spark.util.random

import org.apache.commons.math3.random.{RandomGenerator, RandomGeneratorFactory}

// copied from org.apache.spark.sql.catalyst.expressions.Rand
// adapted to apache commons math3 RandomGenerator
class XORShiftRandomAdapted(init: Long) extends java.util.Random(init: Long) with RandomGenerator {
  def this() = this(System.nanoTime)

  private var seed = XORShiftRandom.hashSeed(init)

  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long): Unit = {
    seed = XORShiftRandom.hashSeed(s)
  }

  override def setSeed(s: Int): Unit = {
    seed = XORShiftRandom.hashSeed(s.toLong)
  }

  override def setSeed(seed: Array[Int]): Unit = {
    this.seed = XORShiftRandom.hashSeed(RandomGeneratorFactory.convertToLong(seed))
  }
}

