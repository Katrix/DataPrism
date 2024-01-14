---
title: Table definitions
---

# {{page.title}}

The first thing you need when using DataPrism is a description of the table you are querying. There
are theoretically many ways to define a table definition, but in 99% of cases, you're probably going
to use some Higher Kinded Data (HKD) structure. HKD is a normal `case class` where the type of each
field is wrapped in a higher kinded type.

Here's an example of some HKD.

```scala sc-name:User.scala
case class UserK[F[_]](
  id: F[Int],
  name: F[Option[String]],
  username: F[String],
  email: F[String]
)
```

You also need `perspective.ApplyKC` and `perspective.TraverseKC` instances for the HKD. Eventually you'll just
be able to use a derive clause for this, but for now you have to call a macro manually.

```scala sc-name:UserInstance.scala sc-compile-with:User.scala
import dataprism.KMacros
// Snippet compiler fails here sadly
given KMacros.ApplyTraverseKC[UserK] = ??? // KMacros.deriveApplyTraverseKC[UserK]
```

Lastly you need the table definition itself. We need an instance of `Table[Codec, User]`. `Codec` here represents how
data is read from and written to the database. The codec you use depends on how you connect to the database. Currently,
there are two options: JDBC and skunk. Depending on your choice, you'll use different database codecs. Unless specified
otherwise, in these docs I will assume JDBC is being used.

`Table` itself takes two parameters. The name of the table, and a value of type `User[Column]`. `Column` meanwhile takes
two parameters, the name and the type of the column. The types can be gotten from an object that corresponds to the
database you're using in `dataprism.jdbc.sql.<db choice>JdbcTypes`. For these examples I will assume Postgres is being
used.

Here is how a table definition could look like for our HKD above.

```scala sc-compile-with:UserInstance.scala
import dataprism.sql.{Table, Column}
import dataprism.jdbc.sql.JdbcCodec
import dataprism.jdbc.sql.PostgresJdbcTypes.*

val table: Table[JdbcCodec, UserK] = Table(
  "users",
  UserK(
    Column("id", integer),
    Column("name", text.nullable),
    Column("username", text),
    Column("email", text)
  )
)
```

Notice that all types have to be written out by hand. This is by design, and a similar approach is taken elsewhere it
DataPrism. Types are never inferred. You have to specify them.

Putting it all together, our table definition could look like this.
```scala
import dataprism.KMacros
import dataprism.sql.{Table, Column}
import dataprism.jdbc.sql.JdbcCodec
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
```
