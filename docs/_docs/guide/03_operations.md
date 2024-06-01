---
title: Operations (SELECT, INSERT, UPDATE, DELETE)
---

# {{page.title}}

After defining some queries, the next step is to run them. Running queries requires an instance of 
`Db[F, Codec]`. These docs will use `DataSourceDb`. 

```scala 3 sc-name:Setup.scala
import dataprism.KMacros
import dataprism.sql.{Table, Column}
import dataprism.jdbc.sql.{JdbcCodec, DataSourceDb}
import dataprism.jdbc.sql.PostgresJdbcTypes.*
import scala.concurrent.Future

case class UserK[F[_]](
  id: F[Int],
  name: F[Option[String]],
  username: F[String],
  email: F[String]
)

object UserK:
  // Snippet compiler fails here sadly
  given KMacros.ApplyTraverseKC[UserK] = ??? // KMacros.deriveApplyTraverseKC[UserK]

  val table: Table[JdbcCodec, UserK] = Table(
    "users",
    UserK(
      Column("id", integer),
      Column("name", text.nullable),
      Column("username", text),
      Column("email", text)
    )
  )

import scala.concurrent.ExecutionContext.Implicits.global

given DataSourceDb[Future] = DataSourceDb.ofFuture(???)
```

## Select

The simplest operation is `Select`, used like so.

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import scala.concurrent.Future
import perspective.Id

val data: Future[Seq[UserK[Id]]] = Select(Query.from(UserK.table)).run
```

## Insert

Next up is insert. There are a few ways to do this depending on what you want to insert. Do you
insert values from outside or inside the database. Do you want to insert into all the columns of the 
table, or leave some for the database to handle?

The simples way is just `Insert.into(table).values(value, values*)`. For this function you pass in the table 
to insert, and values to insert. The next option is `Insert.into(table).valuesFromQuery(query)`.

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

Insert.into(UserK.table).values(UserK(5, Some("foo"), "bar", "foo@example.com")).run

Insert.into(UserK.table).valuesFromQuery(
  Query.from(UserK.table).filter(_.username === "foo".as(text))
).run
```

Sometimes you only want to insert values into some columns. To do this, use 
`valuesInColumns(projection)(value, values*)` or `valuesInColumnsFromQuery(projection)(query)`. 
These functions take a projection function (simplified as) `A[Column] => B[Column]`. `A` is the type 
of the table while `B` is  a projection to the columns you want to set. From there you pass in 
either a `Query[B]` or values of type `B[Id]`..

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import perspective.Compose2

Insert
  .into(UserK.table)
  .valuesInColumns(u => (u.name, u.username, u.email))((None, "bar", "bar@example.com"))
  .run

Insert
  .into(UserK.table)
  .valuesInColumnsFromQuery(u => (u.name, u.username, u.email))(
    Query.from(UserK.table)
      .filter(_.username === "foo".as(text))
      .map(user => (user.name, user.username, user.email))
  )
  .run
```

## Update

Updates are yet a bit more complex than Inserts, and offer more choices, mostly because you can
optionally add a `FROM` clause to the generated SQL in addition to only setting some columns, 
as was done with inserts. There are in general four forms

* `Update.table(table).where(cond).values(update)`
* `Update.table(table).where(cond).valuesInColumns(projection)(partialUpdate)`
* `Update.table(table).from(table2).where(cond).values(partialUpdate)`
* `Update.table(table).from(table2).where(cond).valuesInColumns(projection)(partialUpdate)`

Here are some examples:

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import perspective.Compose2

Update
  .table(UserK.table)
  .where(_.username === "foo".as(text))
  .values(u =>u.copy(name = u.username.asSome))
  .run

Update
  .table(UserK.table)
  .where(_.username === "foo".as(text))
  .valuesInColumns(user => user.name)(user => user.username.asSome)
  .run

Update
  .table(UserK.table)
  .from(Query.from(UserK.table))
  .where((a, b) => a.username === "foo".as(text) && b.username === "bar".as(text))
  .values((a, b) => a.copy(name = b.name))
  .run

Update
  .table(UserK.table)
  .from(Query.from(UserK.table))
  .where((a, b) => a.username === "foo".as(text) && b.username === "bar".as(text))
  .valuesInColumns(u => u.email)((a, b) =>b.email)
  .run
```

## Delete

Lastly deletes. They are luckily a bit simpler. You can still add a `USING` clause, but there is
nothing about only
affecting some columns. Here are some examples of how Delete works.

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}

Delete.from(UserK.table).where(_.username === "foo".as(text)).run
Delete.from(UserK.table).using(Query.from(UserK.table)).where { (a, b) =>
  a.username === b.username
}.run
```
