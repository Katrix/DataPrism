---
title: Usage with Fs2
---

# {{page.title}}

DataPrism has integrations with fs2 in the module `dataprism-fs2` to get the results of a query as a
stream. The JDBC Implementation for this module can be found in the `dataprism-jdbc-fs2` module. To
get nice functions to run an `Operation` as a stream, you need to create your own platform, and mix
in `Fs2SqlQueryPlatform`. Here's an example.

```scala 3
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.platform.sql.Fs2SqlQueryPlatform

object MyPlatform extends PostgresJdbcPlatform, Fs2SqlQueryPlatform
```

From there you create the stream aware `Db` using a `DataSource` like normal.

```scala 3
import dataprism.jdbc.sql.*
import cats.effect.IO
import javax.sql.DataSource

val ds: DataSource = ???
given Fs2DataSourceDb[IO] = new Fs2DataSourceDb(ds)
```