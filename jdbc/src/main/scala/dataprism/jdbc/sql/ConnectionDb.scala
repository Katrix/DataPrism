package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, SQLException}

import scala.util.{Try, Using}

import cats.Functor
import cats.data.{State, ValidatedNel}
import cats.kernel.Monoid
import cats.syntax.all.*
import dataprism.sql.*
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}

trait ConnectionDb[F[_]: Functor] extends Db[F, JdbcCodec]:

  protected def wrapTry[A](tryV: => Try[A]): F[A]

  protected def getConnection(using ResourceManager): Connection

  protected def makePrepared(sql: SqlStr[JdbcCodec], con: Connection)(using ResourceManager): PreparedStatement =
    con.prepareStatement(sql.str).acquire

  protected def setArgs(con: Connection, ps: PreparedStatement, args: Seq[SqlArg[JdbcCodec]], batch: Int)(
      using ResourceManager
  ): Unit =
    for (obj, i) <- args.zipWithIndex do obj.codec.set(ps, i + 1, obj.value(batch), con)

  protected def makePreparedWithBatch[A: Monoid](sql: SqlStr[JdbcCodec], con: Connection, trueBatch: Boolean)(
      use: PreparedStatement => A
  )(
      using ResourceManager
  ): A =
    val ps = makePrepared(sql, con)
    sql.args.map(_.batchSize).distinct match
      case Seq() => use(ps)
      case Seq(batchSize) =>
        val doBatch = trueBatch && batchSize > 1

        var res   = Monoid[A].empty
        var batch = 0
        while batch < batchSize do
          setArgs(con, ps, sql.args, batch)
          if doBatch then ps.addBatch() else res = res.combine(use(ps))
          batch += 1

        if doBatch then use(ps) else res
      case sizes => throw new SQLException(s"Multiple batch sizes: ${sizes.mkString(" ")}")

  protected def makeAndUsePrepared[A: Monoid](
      sql: SqlStr[JdbcCodec],
      trueBatch: Boolean = false
  )(f: ResourceManager ?=> (PreparedStatement, Connection) => A): F[A] = wrapTry {
    Using.Manager { implicit man =>
      given ResourceManager = ResourceManager.proxyForUsingManager
      val con               = getConnection
      makePreparedWithBatch(sql, con, trueBatch)(ps => f(ps, con))
    }
  }

  override def run(sql: SqlStr[JdbcCodec]): F[Int] = makeAndUsePrepared(sql)((ps, _) => ps.executeUpdate())

  override def runBatch(sql: SqlStr[JdbcCodec]): F[Seq[Int]] =
    makeAndUsePrepared(sql, trueBatch = true) { (ps, _) =>
      sql.args.map(_.batchSize).distinct match
        case Seq(batchSize) if batchSize > 1 => ps.executeBatch().toSeq
        case _                               => Seq(ps.executeUpdate())
    }

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
