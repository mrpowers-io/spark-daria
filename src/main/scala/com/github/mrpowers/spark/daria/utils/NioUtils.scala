package com.github.mrpowers.spark.daria.utils

import java.io.IOException
import java.nio.file.{Files, Paths, Path, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes

// Copied from StackOverflow: https://stackoverflow.com/a/47380288/1125159
// Per Vladimir Matveev, it's best to use the Java NIO.2 API for modern Java I/O
object NioUtils {

  def remove(root: Path, deleteRoot: Boolean = true): Unit =
    Files.walkFileTree(
      root,
      new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exception: IOException): FileVisitResult = {
          if (deleteRoot) Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )

  def removeUnder(string: String): Unit = remove(Paths.get(string), deleteRoot = false)

  def removeAll(string: String): Unit = remove(Paths.get(string))

  def removeUnder(file: java.io.File): Unit = remove(file.toPath, deleteRoot = false)

  def removeAll(file: java.io.File): Unit = remove(file.toPath)

}
