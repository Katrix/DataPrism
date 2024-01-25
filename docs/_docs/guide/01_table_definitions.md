---
title: Table definitions
---

# {{page.title}}

The first thing you need when using DataPrism is a description of the table you are querying.
In the vast amount of cases, you will express this using a Higher Kinded Data (HKD) structure.
HKD is a normal `case class` where the type of each field is wrapped in a higher kinded type.

Here's an example of a HKD structure.

```scala 3 sc-name:User.scala
case class UserK[F[_]](
  id: F[Int],
  name: F[Option[String]],
  username: F[String],
  email: F[String]
)
```

You also need instances
of [`perspective.ApplyKC`](https://perspective.katsstuff.net/api/perspective/ApplyK.html)
and [`perspective.TraverseKC`](https://perspective.katsstuff.net/api/perspective/TraverseK$.html)
for the HKD. (Eventually you'll just be able to use a derive clause for this, but for now you have
to call a macro manually.)

```scala 3 sc-name:UserInstance.scala sc-compile-with:User.scala
import dataprism.KMacros

// Snippet compiler fails here sadly
given KMacros.ApplyTraverseKC[UserK] = ??? // KMacros.deriveApplyTraverseKC[UserK]
```

Lastly you need the table definition itself, an instance of `Table[Codec, UserK]`. `Codec`
here represents how
data is read from and written to the database. The codec you use depends on how you connect to the
database. Currently,
there are two options: JDBC and Skunk. Depending on your choice, you'll use different database
codecs. Unless specified
otherwise, these docs will assume JDBC is being used.

`Table` itself takes two parameters. The name of the table, and a value of
type `UserK[Column]`. `Column` meanwhile takes
two parameters, the name and the type of the column. The types can be gotten from an object that
corresponds to the
database you're using in `dataprism.jdbc.sql.<db choice>JdbcTypes`. For these docs, Postgres will be
used.

Here is how a table definition could look like for the HKD above.

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

Notice that all database types have to be written out by hand. This is by design, and a similar
approach is taken elsewhere in DataPrism. These types are never inferred. You have to specify them.

Putting it all together, the table definition could look like this.

```scala 3
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

## Nesting

Nesting HKD is completely fine, as long as you derive instances for `ApplyKC` and `TraverseKC` for
all the fields.

```scala 3
import dataprism.KMacros

case class OuterK[F[_]](
  a: F[Int],
  b: F[String],
  inner: OuterK.InnerK[F]
)

object OuterK:
  given KMacros.ApplyTraverseKC[OuterK] = ??? // KMacros.deriveApplyTraverseKC[OuterK]

  case class InnerK[F[_]](
    c: F[Double]
  )

  object InnerK:
    given KMacros.ApplyTraverseKC[InnerK] = ??? // KMacros.deriveApplyTraverseKC[InnerK]
```

## Autoderiving table instances

There is also some experimental support for deriving `Table` instances automatically. Column names
are taken from the name of a field or from an annotation, while column types are gotten from the
type of the field or an annotation. If the SQL types are gotten from the types of the field, you
have to define the field type as `jdbcType.T`. Here is an example:

```scala 3
import dataprism.KMacros
import dataprism.sql.{named, Table}
import dataprism.jdbc.sql.{JdbcCodec, JdbcColumns, jdbcType}
import dataprism.jdbc.sql.PostgresJdbcTypes.*

case class AutoDerivedTableK[F[_]](
  foo: F[text.T],
  bar: F[integer.nullable.T],
  @jdbcType(boolean) baz: F[Boolean],
  @named("bin") @jdbcType(doublePrecision) abc: F[Double]
) //derives JdbcColumns //// Snippet compiler fails here sadly
object AutoDerivedTable:
  given KMacros.ApplyTraverseKC[AutoDerivedTableK] = ??? // KMacros.deriveApplyTraverseKC[AutoDerivedTableK]
  
  //Can't call because snipper compiler failed
  //val table: Table[JdbcCodec, AutoDerivedTableK] = JdbcColumns.table("auto_table") 
```
