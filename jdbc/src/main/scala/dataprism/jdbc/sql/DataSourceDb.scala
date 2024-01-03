package dataprism.jdbc.sql

import cats.data.State
import dataprism.sql.*
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

import java.io.Closeable
import java.sql.{Connection, PreparedStatement}
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

class DataSourceDb(ds: DataSource)(using ExecutionContext) extends Db[Future, JdbcType]:

  extension [A: Using.Releasable](a: A)
    def acquired(using man: Using.Manager): A =
      man.acquire(a)
      a

  private def makePrepared[A](sql: SqlStr[JdbcType])(f: Using.Manager ?=> (PreparedStatement, Connection) => A): Future[A] =
    Future {
      Using.Manager { implicit man =>
        println("Making prepared statement")
        println(sql.str)

        val con = ds.getConnection.acquired
        val ps = con.prepareStatement(sql.str).acquired

        for ((obj, i) <- sql.args.zipWithIndex) {
          obj.tpe.set(ps, i + 1, obj.value, con)
        }

        f(ps, con)
      }
    }.flatMap(Future.fromTry)

  override def run(sql: SqlStr[JdbcType]): Future[Int] = makePrepared(sql)((ps, _) => ps.executeUpdate())

  override def runIntoSimple[Res](
      sql: SqlStr[JdbcType],
      dbTypes: JdbcType[Res]
  ): Future[QueryResult[Res]] =
    runIntoRes[ProductKPar[Tuple1[Res]]](sql, ProductK.ofScalaTuple[JdbcType, Tuple1[Res]](Tuple1(dbTypes)))
      .map(_.map(_.tuple.head))

  override def runIntoRes[Res[_[_]]](
      sql: SqlStr[JdbcType],
      dbTypes: Res[JdbcType]
  )(using FT: TraverseKC[Res]): Future[QueryResult[Res[Id]]] =
    makePrepared(sql) { (ps: PreparedStatement, con: Connection) =>
      val rs = ps.executeQuery().acquired

      val indicesState: State[Int, Res[Tuple2K[JdbcType, Const[Int]]]] =
        dbTypes.traverseK([A] => (tpe: JdbcType[A]) => State((acc: Int) => (acc + 1, (tpe, acc))))

      val indices: Res[Tuple2K[JdbcType, Const[Int]]] = indicesState.runA(1).value

      val rows = Seq.unfold(rs.next())(cond =>
        Option.when(cond)(
          (
            indices.mapK[Id](
              [A] => (t: (JdbcType[A], Int)) => t._1.get(rs, t._2, con)
            ),
            rs.next()
          )
        )
      )

      QueryResult(rows)
    }
