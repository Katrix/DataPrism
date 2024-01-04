lazy val commonSettings = Seq(
  scalaVersion := "3.3.1"
)

inThisBuild(
  Seq(
    homepage      := Some(url("https://github.com/Katrix/DataPrism")),
    organization  := "net.katsstuff",
    licenses      := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    developers    := List(Developer("Katrix", "Kathryn Frid", "katrix97@hotmail.com", url("http://katsstuff.net/"))),
    versionScheme := Some("early-semver")
  )
)

lazy val publishSettings = Seq(
  Test / publishArtifact := false,
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/Katrix/DataPrism"),
      "scm:git:github.com/Katrix/DataPrism",
      Some("scm:git:github.com/Katrix/DataPrism")
    )
  ),
  autoAPIMappings := true
)

lazy val noPublishSettings = Seq(publish := {}, publishLocal := {}, publishArtifact := false, publish / skip := true)

lazy val common = project.settings(
  commonSettings,
  publishSettings,
  name                                   := "dataprism-common",
  libraryDependencies += "net.katsstuff" %% "perspective"            % "0.2.0",
  libraryDependencies += "net.katsstuff" %% "perspective-derivation" % "0.2.0"
)

lazy val jdbc = project
  .settings(
    commonSettings,
    publishSettings,
    name := "dataprism-jdbc"
  )
  .dependsOn(common)

lazy val skunk = project
  .settings(
    commonSettings,
    publishSettings,
    name                                  := "dataprism-skunk",
    libraryDependencies += "org.tpolecat" %% "skunk-core" % "0.6.2"
  )
  .dependsOn(common)

lazy val docs = project
  .enablePlugins(ScalaUnidocPlugin)
  .settings(
    commonSettings,
    autoAPIMappings := true,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      common,
      jdbc,
      skunk
    ),
    ScalaUnidoc / unidoc / scalacOptions ++= Seq(
      "-sourcepath",
      (LocalRootProject / baseDirectory).value.getAbsolutePath,
      "-siteroot",
      "docs",
      "-project",
      "DataPrism",
      "-project-version",
      version.value,
      "-social-links:github::https://github.com/Katrix/DataPrism",
      "-source-links:github://Katrix/DataPrism",
      "-revision",
      "main",
      "-Yapi-subdirectory",
      "api"
    )
  )

lazy val dataprismRoot = project.in(file(".")).aggregate(common, jdbc, skunk).settings(noPublishSettings)
