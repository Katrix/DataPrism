---
title: Queries
---

# {{page.title}}

With the user type and table definition in hard, the next step is to construct some queries using this table.

```scala 3 sc-name:User.scala
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

For basically anything that manipulates database values, one needs a `QueryPlatform`. Once again,
you
should select a platform based on the database and codec type you are using. From here on, unless
specified otherwise, these docs will use `PostgresJdbcPlatform`.

## Query.from, map, filter, limit, offset

A query can be constructed using `Query.from(table)`. From there you can do various things on the
query, like map, filter, grouping and more. Here are some simple examples. The types of the values
are annotated for better understanding.

```scala 3 sc-compile-with:User.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.*

val q: Query[UserK] = Query.from(UserK.table)

val q2: Query[UserK] = q.map((u: UserK[DbValue]) =>
  u.copy(name = u.name.getOrElse(u.username).asSome)
)

val q3: Query[UserK] = q.filter((u: UserK[DbValue]) => u.name.isDefined)

val v1: DbValue[Long] = q.size

val q4: Query[[F[_]] =>> F[String]] = q.map((u: UserK[DbValue]) => u.username)

val q5: Query[[F[_]] =>> (F[String], F[String])] = q.map(
  (u: UserK[DbValue]) => (u.username, u.email)
)

val q6: Query[UserK] = q.drop(2).limit(5).offset(3)
``` 

Within queries, all values above are wrapped in `DbValue`. This type does however not appear it the
`Query` type. The `Query` type only knows about the abstract HKD type.

## Joins

Not all queries operate on plain `DbValue`s. Joins are one example. While full (normal) joins
require both sides to be present, other join types might make one side nullable. This is represented
by the `Nullable` type.

```scala 3
type Nullable[A] <: Option[_] = A match {
  case Option[b] => Option[b]
  case _ => Option[A]
}
```

`Nullable` wraps any type in `Option`, unless it is already wrapped in `Option`. For example:

* `Nullable[Int]` is `Option[Int]`
* `Nullable[Option[Int]]` is `Option[Int]`

Here is an example how `UserK[Nullable]` would look as a plain case class (take note of how
`Nullable` keeps the type of `name` the same).

```scala 3
// A nullable user. That is to say UserK[Nullable] 
case class NullableUser(
  id: Option[Int],
  name: Option[String],
  username: Option[String],
  email: Option[String]
)
```

With that, here are the `Query` types for joins.

```scala 3 sc-compile-with:User.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.*

// Generally you'd just import perspective.Compose2
type Compose2[A[_], B[_]] = [X] =>> A[B[X]]

val q: Query[UserK] = Query.from(UserK.table)

val innerJoin: Query[[F[_]] =>> (UserK[F], UserK[F])] = q.join(q)(_.username === _.username)

val leftJoin: Query[[F[_]] =>> (UserK[F], UserK[Compose2[F, Nullable]])] =
  q.leftJoin(UserK.table)(_.username === _.username)

val rightJoin: Query[[F[_]] =>> (UserK[Compose2[F, Nullable]], UserK[F])] =
  q.rightJoin(UserK.table)(_.username === _.username)

val fullJoin: Query[[F[_]] =>> (UserK[Compose2[F, Nullable]], UserK[Compose2[F, Nullable]])] =
  q.fullJoin(UserK.table)(_.username === _.username)
```

As can also be seen in this example, you can join on a query, or directly on a table.

## groupMap

The last query function to look at is `groupMap`. DataPrism does not expose a
traditional `groupBy` function, as a `groupMap` function maps better to DataPrism's style.
`groupMap` takes two functions as arguments. The first one exctracts the values to group by.
The second function does the aggregation given both the extracted value, and the values of the
query, which are now wrapped in `Many`. Here are some examples:

```scala 3 sc-compile-with:User.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.*

//Needed for arrayAgg currently
import dataprism.jdbc.sql.PostgresJdbcTypes.ArrayMapping.given_ArrayMapping_A

val q: Query[UserK] = Query.from(UserK.table)

val q1: Query[[F[_]] =>> (F[String], F[Seq[String]])] =
  q.groupMap((v: UserK[DbValue]) => v.email)(
    (email: DbValue[String], v: UserK[Many]) => (email, v.username.arrayAgg)
  )

val q2: Query[[F[_]] =>> (F[Option[String]], F[String], F[Seq[String]])] =
  q.groupMap((v: UserK[DbValue]) => (v.name, v.username))(
    (t: (DbValue[Option[String]], DbValue[String]), v: UserK[Many]) =>
      (t._1, t._2, v.email.arrayAgg)
  )
```

Note how you don't have to directly return a column from the grouping function. For example, in `q3`
a tuple is used. Anything that works for `map` also works here.

## flatMap

So far the docs have looked at direct function application on the queries, resulting in an
applicative style. `Query` also defines `flatMap`, and because of that for comprehensions.

```scala 3 sc-compile-with:User.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.*

//TODO: Does not compile for some reason. Fix MapRes
//val q1: Query[[F[_]] =>> (UserK[F], UserK[F])] =
//  Query.from(UserK.table).flatMap(u1 => Query.from(UserK.table).map(u2 => (u1, u2)))

val q2: Query[UserK] = for
  u <- Query.from(UserK.table)
  u2 <- Query.from(UserK.table)
  if u.email === u2.email
yield u2
```

## Mapping to other Higher Kinded Data

These examples have generally shown either `UserK` or a tuple in functions like `map`, `groupMap`
and similar. The HKD used to define a table is not special. Any HKD (or not even HKD)
with `perspective.ApplyKC` and `perspective.TraverseKC` instances can be used as a result type in
functions like `map`. Here's one example:

```scala 3 sc-compile-with:User.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.*

//Needed for arrayAgg currently
import dataprism.jdbc.sql.PostgresJdbcTypes.ArrayMapping.given_ArrayMapping_A

case class UsersWithEmailK[F[_]](email: F[String], usernames: F[Seq[String]])

object UsersWithEmailK:
  // Snippet compiler fails here sadly
  given KMacros.ApplyTraverseKC[UsersWithEmailK] = ??? // KMacros.deriveApplyTraverseKC[UsersWithEmailK]

val q1: Query[UsersWithEmailK] =
  Query.from(UserK.table).groupMap((v: UserK[DbValue]) => v.email)(
    (email: DbValue[String], v: UserK[Many]) => UsersWithEmailK(email, v.username.arrayAgg)
  )
```

For more info, see [MapRes and Exotic data](/07_mapres_exotic_data.md)
