package com.github.mrpowers.spark.daria

class CustomFramework extends utest.runner.Framework {
  override def formatWrapWidth: Int = 300
  // turn off the default exception message color, so spark-fast-tests
  // can send messages with custom colors
  override def exceptionMsgColor    = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionPrefixColor = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionMethodColor = toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionPunctuationColor =
    toggledColor(utest.ufansi.Attrs.Empty)
  override def exceptionLineNumberColor = toggledColor(utest.ufansi.Attrs.Empty)
}
