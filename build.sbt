lazy val commonSettings = Seq(
  scalaVersion := "3.3.5",
  resolvers ++= Resolver.sonatypeOssRepos("snapshots"),
  scalacOptions ++= Seq("-feature", "-deprecation", "-unchecked" /*, "-explain"*/ )
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
  name                                         := "dataprism-common",
  libraryDependencies += "net.katsstuff"       %% "perspective"            % "0.3.0",
  libraryDependencies += "net.katsstuff"       %% "perspective-derivation" % "0.3.0",
  libraryDependencies += "com.disneystreaming" %% "weaver-cats"            % "0.8.4"  % Test,
  libraryDependencies += "com.disneystreaming" %% "weaver-scalacheck"      % "0.8.4"  % Test,
  libraryDependencies += "io.chrisdavenport"   %% "cats-scalacheck"        % "0.3.2"  % Test,
  libraryDependencies += "org.typelevel"       %% "spire"                  % "0.18.0" % Test,
  testFrameworks += new TestFramework("weaver.framework.CatsEffect")
)

lazy val jdbc = project
  .settings(
    commonSettings,
    publishSettings,
    name := "dataprism-jdbc",
    libraryDependencies ++= Seq(
      "org.typelevel"     %% "cats-effect"         % "3.5.3"    % Test,
      "org.postgresql"     % "postgresql"          % "42.7.1"   % Test,
      "com.mysql"          % "mysql-connector-j"   % "8.3.0"    % Test,
      "org.mariadb.jdbc"   % "mariadb-java-client" % "3.3.3"    % Test,
      "org.testcontainers" % "testcontainers"      % "1.19.5"   % Test,
      "org.testcontainers" % "mysql"               % "1.19.5"   % Test,
      "org.testcontainers" % "postgresql"          % "1.19.5"   % Test,
      "org.testcontainers" % "mariadb"             % "1.19.5"   % Test,
      "com.h2database"     % "h2"                  % "2.2.224"  % Test,
      "org.xerial"         % "sqlite-jdbc"         % "3.45.2.0" % Test,
      "org.slf4j"          % "slf4j-simple"        % "2.0.12"   % Test
    ),
    Test / fork := true
  )
  .dependsOn(common % "compile->compile;test->test")

lazy val cats = project
  .settings(
    commonSettings,
    publishSettings,
    name                                   := "dataprism-cats",
    libraryDependencies += "org.typelevel" %% "cats-effect-kernel" % "3.5.3"
  )
  .dependsOn(common)

lazy val fs2 = project
  .settings(
    commonSettings,
    publishSettings,
    name                            := "dataprism-fs2",
    libraryDependencies += "co.fs2" %% "fs2-core" % "3.9.4"
  )
  .dependsOn(cats)

lazy val jdbcCats = project
  .settings(
    commonSettings,
    publishSettings,
    name := "dataprism-jdbc-cats"
  )
  .dependsOn(jdbc, cats)

lazy val jdbcFs2 = project
  .settings(
    commonSettings,
    publishSettings,
    name := "dataprism-jdbc-fs2"
  )
  .dependsOn(fs2, jdbcCats)

lazy val skunk = project
  .settings(
    commonSettings,
    publishSettings,
    name                                  := "dataprism-skunk",
    libraryDependencies += "org.tpolecat" %% "skunk-core" % "0.6.4",
    libraryDependencies ++= Seq(
      "org.testcontainers" % "testcontainers" % "1.19.5" % Test,
      "org.testcontainers" % "postgresql"     % "1.19.5" % Test,
      "org.slf4j"          % "slf4j-simple"   % "2.0.12" % Test
    )
  )
  .dependsOn(common % "compile->compile;test->test", cats, fs2)

lazy val docs = project
  .enablePlugins(ScalaUnidocPlugin)
  .settings(
    commonSettings,
    // scalaVersion := "3.5.0-RC1",
    libraryDependencies += "org.typelevel" %% "cats-effect-std" % "3.5.3",
    autoAPIMappings                        := true,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(
      common,
      jdbc,
      cats,
      jdbcCats,
      fs2,
      jdbcFs2,
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
      // "api",
      "-Ygenerate-inkuire",
      "-snippet-compiler:compile"
    )
  )

lazy val dataprismRoot =
  project.in(file(".")).aggregate(common, jdbc, cats, jdbcCats, fs2, jdbcFs2, skunk).settings(noPublishSettings)
