---
title: DataPrism
---

# {{page.title}}

*A new FRM with focus on Higher Kinded Data*

DataPrism is an SQL query construction library built to take full advantage of
the power of higher kinded data. DataPrism builds
on [`perspective`](https://github.com/Katrix/perspective)
and the tools it provides for programming with higher kinded data.

The power of higher kinded data makes DataPrism more flexible than other Scala SQL libraries.
Want to sometimes leave out a column? You can do that. Want to return a List from a query?
Sure thing.

DataPrism works with both Java's JDBC and Skunk.

Add DataPrism to your project by adding these statements to your `build.sbt` file.
```scala sc:nocompile
// For JDBC
libraryDependencies += "net.katsstuff" %% "dataprism-jdbc" % "{{projectVersion}}"

// For Skunk
libraryDependencies += "net.katsstuff" %% "dataprism-skunk" % "{{projectVersion}}"
```

*DataPrism is currently early in development, but feel free to try it out and
report bugs and errors.*