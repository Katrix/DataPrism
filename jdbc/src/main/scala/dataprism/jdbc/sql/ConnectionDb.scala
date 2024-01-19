package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, SQLException}

import scala.util.Using

import cats.Functor
import cats.data.{State, ValidatedNel}
import cats.syntax.all.*
import dataprism.sql.*
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait ConnectionDb[F[_]: Functor] extends Db[F, JdbcCodec]:

  extension [A: Using.Releasable](a: A)
    def acquired(using man: Using.Manager): A =
      man.acquire(a)
      a

  protected def makePrepared[A](
      sql: SqlStr[JdbcCodec]
  )(f: Using.Manager ?=> (PreparedStatement, Connection) => A): F[A]

  override def run(sql: SqlStr[JdbcCodec]): F[Int] = makePrepared(sql)((ps, _) => ps.executeUpdate())

  override def runIntoSimple[Res](
      sql: SqlStr[JdbcCodec],
      dbTypes: JdbcCodec[Res]
  ): F[Seq[Res]] =
    runIntoRes[ProductKPar[Tuple1[Res]]](sql, ProductK.ofScalaTuple[JdbcCodec, Tuple1[Res]](Tuple1(dbTypes)))
      .map(_.map(_.tuple.head))

  override def runIntoRes[Res[_[_]]](
      sql: SqlStr[JdbcCodec],
      dbTypes: Res[JdbcCodec],
      minRows: Int = 0,
      maxRows: Int = -1
  )(using FT: TraverseKC[Res]): F[Seq[Res[Id]]] =
    makePrepared(sql) { (ps: PreparedStatement, con: Connection) =>
      val rs = ps.executeQuery().acquired

      val indicesState: State[Int, Res[Tuple2K[JdbcCodec, Const[Int]]]] =
        dbTypes.traverseK([A] => (tpe: JdbcCodec[A]) => State((acc: Int) => (acc + 1, (tpe, acc))))

      val indices: Res[Tuple2K[JdbcCodec, Const[Int]]] = indicesState.runA(1).value

      val rows = Seq
        .unfold((rs.next(), 0))((cond, read) =>
          Option.when(cond) {
            if maxRows >= 0 && read > maxRows then throw new SQLException(s"Expected only $maxRows rows, but got more")

            (
              indices.traverseK[[A] =>> ValidatedNel[String, A], Id](
                [A] => (t: (JdbcCodec[A], Int)) => t._1.get(rs, t._2, con).toValidatedNel
              ),
              (rs.next(), read + 1)
            )
          }
        )
        .sequence
        .valueOr(es => throw new SQLException(s"Failed to map columns: ${es.toList.mkString(", ")}"))

      if rows.length < minRows then throw new SQLException(s"Expected at least $minRows, but got ${rows.length}")

      rows
    }
