logLevel := Level.Warn

resolvers += Resolver.bintrayIvyRepo(
  "s22s",
  "sbt-plugins"
)

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")

addSbtPlugin("com.eed3si9n"     % "sbt-assembly" % "0.15.0")
addSbtPlugin("org.xerial.sbt"   % "sbt-sonatype" % "3.9.10")
addSbtPlugin("com.jsuereth"     % "sbt-pgp"      % "2.0.1")
