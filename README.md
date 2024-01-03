# DataPrism

*A new FRM with focus on Higher Kinded Data*

DataPrism is an SQL query construction library built to take full advantage of
the power of higher kinded data. DataPrism builds on `perspective` and the
tools it provides.

DataPrism is more flexible than other SQL libraries made for Scala. Want to
sometimes leave out a column? You can do that. Want to return a List from a query,
sure thing.

DataPrism also works with both Java's JDBC and skunk.

Add DataPrism to your project by adding these statements to your `build.sbt` file.

DataPrism is currently early in development, but feel free to try it out and
report bugs and errors.

```scala
// For JDBC
libraryDependencies += "net.katsstuff" %% "dataprism-jdbc" % "{{versions.dataprism}}"

// For Skunk
libraryDependencies += "net.katsstuff" %% "dataprism-skunk" % "{{versions.dataprism}}"
```

Simple showcase of code
```scala


case class HomeK[F[_]](
  owner: F[UUID],
  name: F[String],
  createdAt: F[Instant],
  updatedAt: F[Instant],
  x: F[Double],
  y: F[Double],
  z: F[Double],
  yaw: F[Float],
  pitch: F[Float],
  worldUuid: F[UUID]
)

object HomeK {
  import dataprism.jdbc.sql.JdbcType
  import dataprism.jdbc.sql.PostgresJdbcTypes.*
  val table: Table[HomeK, JdbcType] = Table(
    "homes",
    HomeK(
      owner = Column("owner", uuid),
      name = Column("name", text),
      createdAt = Column("created_at", javaTime.timestamptz),
      updatedAt = Column("updated_at", javaTime.timestamptz),
      x = Column("x", doublePrecision),
      y = Column("y", doublePrecision),
      z = Column("z", doublePrecision),
      yaw = Column("yaw", real),
      pitch = Column("pitch", real),
      worldUuid = Column("world_uuid", uuid)
    )
  )

  given KMacros.ApplyTraverseKC[HomeK] = KMacros.deriveApplyTraverseKC[HomeK]
}


Query.from(HomeK.table).map(homes => (homes.owner, homes.name))

Query
  .from(HomeK.table)
  .groupBy(homes => (homes.owner, homes.name))((grouped, homes) =>
    (
      grouped._1,
      grouped._2,
      homes.x.arrayAgg,
      homes.y.arrayAgg,
      homes.z.arrayAgg
    )
  )
```