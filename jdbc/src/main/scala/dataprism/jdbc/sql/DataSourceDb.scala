package dataprism.jdbc.sql

import java.io.Closeable
import java.sql.{Connection, PreparedStatement, SQLException}
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

import cats.data.{State, ValidatedNel}
import cats.syntax.all.*
import dataprism.sql.*
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

class DataSourceDb(ds: DataSource)(using ExecutionContext) extends Db[Future, JdbcCodec]:

  extension [A: Using.Releasable](a: A)
    def acquired(using man: Using.Manager): A =
      man.acquire(a)
      a

  private def makePrepared[A](
      sql: SqlStr[JdbcCodec]
  )(f: Using.Manager ?=> (PreparedStatement, Connection) => A): Future[A] =
    Future {
      Using.Manager { implicit man =>
        val con = ds.getConnection.acquired
        val ps  = con.prepareStatement(sql.str).acquired

        for ((obj, i) <- sql.args.zipWithIndex) {
          obj.tpe.set(ps, i + 1, obj.value, con)
        }

        f(ps, con)
      }
    }.flatMap(Future.fromTry)

  override def run(sql: SqlStr[JdbcCodec]): Future[Int] = makePrepared(sql)((ps, _) => ps.executeUpdate())

  override def runIntoSimple[Res](
      sql: SqlStr[JdbcCodec],
      dbTypes: JdbcCodec[Res]
  ): Future[QueryResult[Res]] =
    runIntoRes[ProductKPar[Tuple1[Res]]](sql, ProductK.ofScalaTuple[JdbcCodec, Tuple1[Res]](Tuple1(dbTypes)))
      .map(_.map(_.tuple.head))

  override def runIntoRes[Res[_[_]]](
      sql: SqlStr[JdbcCodec],
      dbTypes: Res[JdbcCodec]
  )(using FT: TraverseKC[Res]): Future[QueryResult[Res[Id]]] =
    makePrepared(sql) { (ps: PreparedStatement, con: Connection) =>
      val rs = ps.executeQuery().acquired

      val indicesState: State[Int, Res[Tuple2K[JdbcCodec, Const[Int]]]] =
        dbTypes.traverseK([A] => (tpe: JdbcCodec[A]) => State((acc: Int) => (acc + 1, (tpe, acc))))

      val indices: Res[Tuple2K[JdbcCodec, Const[Int]]] = indicesState.runA(1).value

      val rows = Seq
        .unfold(rs.next())(cond =>
          Option.when(cond)(
            (
              indices.traverseK[[A] =>> ValidatedNel[String, A], Id](
                [A] => (t: (JdbcCodec[A], Int)) => t._1.get(rs, t._2, con).toValidatedNel
              ),
              rs.next()
            )
          )
        )
        .sequence
        .valueOr(es => throw new SQLException(s"Failed to map columns: ${es.toList.mkString(", ")}"))

      QueryResult(rows)
    }
