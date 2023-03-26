package dataprism.sql

import java.io.Closeable
import java.sql.{PreparedStatement, ResultSet}
import javax.sql.DataSource

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Using

import cats.data.State
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait Db:

  def run(sql: SqlStr): Future[Int]

  def runIntoSimple[Res](
      sql: SqlStr,
      dbTypes: DbType[Res]
  ): Future[QueryResult[Res]]

  def runIntoRes[Res[_[_]]](
      sql: SqlStr,
      dbTypes: Res[DbType]
  )(using FA: ApplyKC[Res], FT: TraverseKC[Res]): Future[QueryResult[Res[Id]]]
