---
title: Compiled queries and commands
---

# {{page.title}}

DataPrism can, if desired compile an operation so that it only has to create the SQL for it once, instead of again and
again. This generally takes the form of a function accepting types of the arguments to compile, and another function
allowing use of those values as `DbValue`. The two global compile operations are `raw` and `operation`. `raw` works with
raw `SqlStr`s, while `operation` works with operations like SELECT and DELETE. The docs here will only look at operations If
you need raw, you'll know, and hopefully be able to figure out how to use it.

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

## operation

Here are two examples of how operation works. Due to Scala inferring too precise of a type for the `Type` arguments, 
the code calls `forgetNNA` on the types.

```scala 3 sc-compile-with:Setup.scala
import dataprism.jdbc.platform.PostgresJdbcPlatform.Api.{*, given}
import perspective.Id
import scala.concurrent.Future

val f1: String => Future[Seq[UserK[Id]]] =
  Compile.operation(text.forgetNNA) { (textValue: DbValue[String]) =>
    Select(Query.from(UserK.table).filter(_.username === textValue))
  }

val f2: ((String, String)) => Future[Seq[UserK[Id]]] =
  Compile.operation((text.forgetNNA, text.forgetNNA)) {
    (textValue1: DbValue[String], textValue2: DbValue[String]) =>
      Select(
        Query.from(UserK.table).filter { u =>
          u.username === textValue1 && u.email === textValue2
        }
      )
  }
```
