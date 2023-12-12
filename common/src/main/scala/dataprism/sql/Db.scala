package dataprism.sql

import cats.data.State
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

import java.io.Closeable
import java.sql.{PreparedStatement, ResultSet}
import javax.sql.DataSource
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Using

trait Db[F[_], Type[_]]:

  def run(sql: SqlStr[Type]): F[Int]

  def runIntoSimple[Res](
      sql: SqlStr[Type],
      dbTypes: Type[Res]
  ): F[QueryResult[Res]]

  def runIntoRes[Res[_[_]]](
      sql: SqlStr[Type],
      dbTypes: Res[Type]
  )(using FA: ApplyKC[Res], FT: TraverseKC[Res]): F[QueryResult[Res[Id]]]
