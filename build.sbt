lazy val commonSettings = Seq(
  scalaVersion := "3.1.3",
  version      := "0.0.1"
)

lazy val common = project.settings(
  commonSettings,
  name                                   := "dataprism-common",
  libraryDependencies += "net.katsstuff" %% "perspective"            % "0.0.7",
  libraryDependencies += "net.katsstuff" %% "perspective-derivation" % "0.0.7"
)
