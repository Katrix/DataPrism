package dataprism.jdbc

class testing2 {

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

  import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform.*
  import dataprism.sql.QueryResult
  import perspective.Id
  import scala.concurrent.Future

  val f1: String => Future[QueryResult[UserK[Id]]] =
    Compile.operation(text.forgetNNA) { (textValue: DbValue[String]) =>
      Select(Query.from(UserK.table).filter(_.username === textValue))
    }
  val f2: ((String, String)) => Future[QueryResult[UserK[Id]]] =
    Compile.operation((text.forgetNNA, text.forgetNNA)) {
      (textValue1: DbValue[String], textValue2: DbValue[String]) =>
        Select(Query.from(UserK.table).filter(u => u.username === textValue1 && u.email === textValue2))
    }
}
