logLevel := Level.Warn

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")