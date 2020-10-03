logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo(
  "s22s",
  "sbt-plugins"
)

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.4")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")
