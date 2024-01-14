---
title: Operations (SELECT, INSERT, UPDATE, DELETE)
---

# {{page.title}}

Now that we have our queries, it is time to run them. For this we'll need an instance of `Db[F, Codec]`. For our usecase
we'll use `DataSourceDb`. Operations have a few different functions, but only one is interesting for the average
user, `run`.

```scala 3 sc-name:Setup.scala
import dataprism.KMacros
import dataprism.sql.{Table, Column}
import dataprism.jdbc.sql.{JdbcCodec, DataSourceDb}
import dataprism.jdbc.sql.PostgresJdbcTypes.*

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

given DataSourceDb = DataSourceDb(???)
```

## Select

The simplest operation is `Select`, used like so.

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform.*
import scala.concurrent.Future
import dataprism.sql.QueryResult
import perspective.Id

val data: Future[QueryResult[UserK[Id]]] = Select(Query.from(UserK.table)).run
```

## Insert

Next up is insert. There are a few ways to do this depending on what you want to insert. Do you insert values from
outside or inside the database. Do you want to insert into all the columns of the table, or leave some for the database
to handle?

The simples way is just `Insert.values`. For this function you pass in the table to insert, and values to insert. The
next option is `Insert.into(table).values(query)`. `Insert.values` is a convenience function that
calls `Insert.into(table).values(Query.valuesOf(table, firstValues, remainingValues*))`

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform.*

Insert.values(UserK.table, UserK(5, Some("foo"), "bar", "foo@example.com")).run

Insert.into(UserK.table).values(
  Query.from(UserK.table).filter(_.username === "foo".as(text))
).run
```

Sometimes you only want to insert values into some columns. To do this, use `valuesWithoutSomeColumns(query)`. The query
for this can be created using `Query.valueOfOpt`. The arguments for `valueOfOpt` would in this case be the table
definition, and `User[Option]`. You can also construct a query to pass to `valuesWithoutSomeColumns` with just a normal
query and wrapping the desired columns in `Some`

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform.*
import perspective.Compose2

Insert.into(UserK.table).valuesWithoutSomeColumns(
  Query.valueOfOpt(
    UserK.table,
    UserK(
      id = None,
      name = Some(None),
      username = Some("bar"),
      email = Some("bar@example.com")
    )
  )
).run

Insert.into(UserK.table).valuesWithoutSomeColumns(
  Query.from(UserK.table)
    .filter(_.username === "foo".as(text))
    // MapK might be needed here for type inference
    // It does not work with tuples, but is more precise
    .mapK { user =>
      user.copy[Compose2[Option, DbValue]](
        id = None,
        name = Some(user.name),
        username = Some(user.username),
        email = Some(user.email)
      )
    }
).run
```

Note that in the first example, `name` has type `Option[Option[String]]`. The outer `Option` indicates if the column
should be specified or not, while the inner `Option` specifies nullability. This gets explored further in the next
example. `id` is set to `None`, so it's column won't be specified in the insert statement. The types of `name`
and `email` are now `Option[DbValue[String]]`. Note that the `Option` is outside the `DbValue`. `Option` outside means
column won't be set, while `Option` inside means nullability. `name` shows this well, having the
type `Option[DbValue[Option[String]]]`.

## Update

Updates are yet a bit more complex than Insets, and offer more choices, mostly because you can optionally add a `FROM`
clause to the generated SQL in addition to only setting some columns, as we saw with inserts. There are in general four
forms

* `Update.table(table).where(cond).values(update)`
* `Update.table(table).where(cond).someValues(partialUpdate)`
* `Update.table(table).from(table2).where(cond).values(partialUpdate)`
* `Update.table(table).from(table2).where(cond).someValues(partialUpdate)`

Here are some examples:

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform.*
import perspective.Compose2

Update.table(UserK.table).where(_.username === "foo".as(text)).values { u =>
  u.copy(name = u.username.asSome)
}.run

Update.table(UserK.table).where(_.username === "foo".as(text)).someValues { user =>
  UserK(id = None, name = Some(user.username.asSome), username = None, email = None)
}.run

Update.table(UserK.table)
  .from(Query.from(UserK.table))
  .where((a, b) => a.username === "foo".as(text) && b.username === "bar".as(text))
  .values((a, b) => a.copy(name = b.name))
  .run

Update.table(UserK.table)
  .from(Query.from(UserK.table))
  .where((a, b) => a.username === "foo".as(text) && b.username === "bar".as(text))
  .someValues { (a, b) =>
    UserK(id = None, name = None, username = None, email = Some(b.email))
  }
  .run
```

## Delete

Lastly deletes. They are luckily a bit simpler. You can still add a `USING` clause, but there is nothing about only
affecting some columns. Here are some examples of how Delete works.

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform.*

Delete.from(UserK.table).where(_.username === "foo".as(text)).run
Delete.from(UserK.table).using(Query.from(UserK.table)).where { (a, b) =>
  a.username === b.username
}.run
```
