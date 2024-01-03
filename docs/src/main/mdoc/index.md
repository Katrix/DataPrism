---
layout: home
title: "DataPrism"
---

{% assign versions = site.data.versions %}

# DataPrism

*A new FRM with focus on Higher Kinded Data*

DataPrism is an SQL query construction library built to take full advantage of 
the power of higher kinded data. DataPrism builds on `perspective` and the 
tools it provides.

DataPrism is more flexible than other SQL libraries made for Scala. Want to 
sometimes leave out a column? You can do that. Want to return a List from a query, 
sure thing.

DataPrism also works with both Java's JDBC and skunk.

DataPrism is currently early in development, but feel free to try it out and 
report bugs and errors.

Add DataPrism to your project by adding these statements to your `build.sbt` file.
```scala
// For JDBC
libraryDependencies += "net.katsstuff" %% "dataprism-jdbc" % "{{versions.dataprism}}"

// For Skunk
libraryDependencies += "net.katsstuff" %% "dataprism-skunk" % "{{versions.dataprism}}"
```

# More information

For more information, either see the examples or the ScalaDoc.
