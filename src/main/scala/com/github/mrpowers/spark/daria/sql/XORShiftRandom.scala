package com.github.mrpowers.spark.daria.sql

import org.apache.commons.math3.random.RandomGenerator
import scala.util.hashing.MurmurHash3
import java.nio.ByteBuffer

// copied from org.apache.spark.sql.catalyst.expressions.Rand
// adapted to apache commons math3 RandomGenerator
class XORShiftRandomAdapted(init: Long) extends java.util.Random(init) with RandomGenerator {

  def this() = this(System.nanoTime)

  private var seed = XORShiftRandom.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
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
    this.seed = XORShiftRandom.hashSeed(seed.foldLeft(0L)((acc, s) => acc ^ s.toLong))
  }
}

object XORShiftRandom {
  private[random] def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(seed).array()
    val lowBits = MurmurHash3.bytesHash(bytes, MurmurHash3.arraySeed)
    val highBits = MurmurHash3.bytesHash(bytes, lowBits)
    (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
  }
}