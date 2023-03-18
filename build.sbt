lazy val commonSettings = Seq(
  scalaVersion := "3.2.2",
  version      := "0.0.1-SNAPSHOT",
  organization := "net.katsstuff",
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots".at(nexus + "content/repositories/snapshots"))
    else Some("releases".at(nexus + "service/local/staging/deploy/maven2"))
  }
)

lazy val publishSettings = Seq(
  publishMavenStyle      := true,
  Test / publishArtifact := false,
  licenses               := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/Katrix/DataPrism"),
      "scm:git:github.com/Katrix/DataPrism",
      Some("scm:git:github.com/Katrix/DataPrism")
    )
  ),
  homepage             := Some(url("https://github.com/Katrix/DataPrism")),
  developers           := List(Developer("Katrix", "Kathryn", "katrix97@hotmail.com", url("http://katsstuff.net/"))),
  pomIncludeRepository := (_ => false),
  autoAPIMappings      := true
)

lazy val noPublishSettings = Seq(publish := {}, publishLocal := {}, publishArtifact := false)

lazy val common = project.settings(
  commonSettings,
  publishSettings,
  name                                   := "dataprism-common",
  libraryDependencies += "net.katsstuff" %% "perspective"            % "0.0.7",
  libraryDependencies += "net.katsstuff" %% "perspective-derivation" % "0.0.7"
)

lazy val root = project.in(file(".")).aggregate(common).settings(noPublishSettings)
