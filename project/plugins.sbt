logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.7-astraea.1")

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")