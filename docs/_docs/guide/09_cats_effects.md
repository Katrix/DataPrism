---
title: Usage with Cats effects
---

# {{page.title}}

DataPrism has integrations with cats-effects through the `dataprism-cats` module. The JDBC
Implementation for this module can be found in the `dataprism-jdbc-cats` module.

For now, the only thing these modules expose is a way to do transactions using `Resource`, and
making a JDBC `Db` for any effect type with a `Sync` instance. Note that you still have to get
a `DataSource` from somewhere else.

```scala 3
import dataprism.jdbc.sql.*
import cats.effect.IO
import javax.sql.DataSource

val ds: DataSource = ???
given CatsDataSourceDb[IO] = new CatsDataSourceDb(ds)
```
