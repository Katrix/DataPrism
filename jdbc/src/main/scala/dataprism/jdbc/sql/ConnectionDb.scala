package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, SQLException}

import scala.util.{Try, Using}

import cats.Functor
import cats.data.{State, ValidatedNel}
import cats.syntax.all.*
import dataprism.sql.*
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait ConnectionDb[F[_]: Functor] extends Db[F, JdbcCodec]:

  protected def wrapTry[A](tryV: => Try[A]): F[A]

  protected def getConnection(using ResourceManager): Connection

  protected def makePrepared[A](sql: SqlStr[JdbcCodec], con: Connection)(
      using ResourceManager
  ): PreparedStatement = {
    val ps = con.prepareStatement(sql.str).acquire

    val batchSizes = sql.args.map(_.batchSize).distinct
    if batchSizes.length != 1 then throw new SQLException(s"Multiple batch sizes: ${batchSizes.mkString(" ")}")
    val batchSize = batchSizes.head

    var batch = 0
    while batch < batchSize do {
      for ((obj, i) <- sql.args.zipWithIndex) {
        obj.tpe.set(ps, i + 1, obj.value(batch), con)
      }
      batch += 1
      if batch < batchSize then ps.addBatch()
    }
    ps
  }

  protected def makeAndUsePrepared[A](
      sql: SqlStr[JdbcCodec]
  )(f: ResourceManager ?=> (PreparedStatement, Connection) => A): F[A] = wrapTry {
    Using.Manager { implicit man =>
      given ResourceManager = ResourceManager.proxyForUsingManager
      val con               = getConnection
      val ps                = makePrepared(sql, con)

      f(ps, con)
    }
  }

  override def run(sql: SqlStr[JdbcCodec]): F[Int] = makeAndUsePrepared(sql)((ps, _) => ps.executeUpdate())

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
    makeAndUsePrepared(sql) { (ps: PreparedStatement, con: Connection) =>
      val rs = ps.executeQuery().acquire

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
