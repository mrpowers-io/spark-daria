logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo(
  "s22s",
  "sbt-plugins"
)
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.7-astraea.1")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.5.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
