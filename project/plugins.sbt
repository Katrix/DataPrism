logLevel                     := Level.Warn
addSbtPlugin("com.47deg"      % "sbt-microsites" % "1.4.3")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always