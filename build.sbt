lazy val commonSettings = Seq(
  scalaVersion := "3.3.1",
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
  autoAPIMappings      := true
)

lazy val noPublishSettings = Seq(publish := {}, publishLocal := {}, publishArtifact := false, publish / skip := true)

lazy val common = project.settings(
  commonSettings,
  publishSettings,
  name                                   := "dataprism-common",
  libraryDependencies += "net.katsstuff" %% "perspective"            % "0.2.0-SNAPSHOT",
  libraryDependencies += "net.katsstuff" %% "perspective-derivation" % "0.2.0-SNAPSHOT"
)

lazy val jdbc = project.settings(
  commonSettings,
  publishSettings,
  name := "dataprism-jdbc"
).dependsOn(common)

lazy val skunk = project.settings(
  commonSettings,
  publishSettings,
  name := "dataprism-skunk",
  libraryDependencies += "org.tpolecat" %% "skunk-core" % "0.6.2"
).dependsOn(common)

lazy val docsMappingsAPIDir = settingKey[String]("Name of subdirectory in site target directory for api docs")

lazy val docs = project
  .enablePlugins(MicrositesPlugin, ScalaUnidocPlugin, GhpagesPlugin)
  .settings(
    commonSettings,
    micrositeName                          := "DataPrism",
    micrositeAuthor                        := "Katrix",
    micrositeDescription                   := "A new FRM with focus on Higher Kinded Data",
    micrositeDocumentationUrl              := "/api/dataprism",
    micrositeDocumentationLabelDescription := "ScalaDoc",
    micrositeHomepage                      := "https://dataprism.katsstuff.net",
    micrositeGithubOwner                   := "Katrix",
    micrositeGithubRepo                    := "DataPrism",
    micrositeGitterChannel                 := false,
    micrositeShareOnSocial                 := false,
    micrositeTheme                         := "pattern",
    ghpagesCleanSite / excludeFilter       := "CNAME",
    micrositePushSiteWith                  := GitHub4s,
    micrositeGithubToken                   := sys.env.get("GITHUB_TOKEN"),
    autoAPIMappings := true,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      common,
      jdbc,
      skunk,
    ),
    docsMappingsAPIDir := "api",
    addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, docsMappingsAPIDir),
    //mdoc / fork := true,
    mdocIn := sourceDirectory.value / "main" / "mdoc",
    //ScalaUnidoc / unidoc / fork := true,
    ScalaUnidoc / unidoc / scalacOptions ++= Seq(
      "-doc-source-url",
      "https://github.com/Katrix/DataPrism/tree/master€{FILE_PATH}.scala",
      "-sourcepath",
      (LocalRootProject / baseDirectory).value.getAbsolutePath
    )
  )

lazy val root = project.in(file(".")).aggregate(common, jdbc, skunk).settings(noPublishSettings)
