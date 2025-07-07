logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo(
  "s22s",
  "sbt-plugins"
)

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.1")
