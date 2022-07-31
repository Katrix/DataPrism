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

class Db(ds: DataSource & Closeable) extends Closeable:

  extension [A: Using.Releasable](a: A)
    def acquired(using man: Using.Manager): A =
      man.acquire(a)
      a

  private def makePrepared[A](sql: SqlStr)(f: Using.Manager ?=> PreparedStatement => A): Future[A] =
    Future {
      Using.Manager { implicit man =>
        println("Making prepared statement")
        println(sql.str)

        val ps = ds.getConnection.acquired.prepareStatement(sql.str).acquired

        for ((obj, i) <- sql.args.zipWithIndex) {
          obj.tpe.set(ps, i + 1, obj.value)
        }

        f(ps)
      }
    }.flatMap(Future.fromTry)

  def run(sql: SqlStr): Future[Int] = makePrepared(sql)(_.executeUpdate())

  def runInto[Res[_[_]]](
      sql: SqlStr,
      dbTypes: Res[DbType]
  )(using FA: ApplicativeKC[Res], FT: TraverseKC[Res]): Future[QueryResult[Res[Id]]] = runIntoRes(sql)(using dbTypes)

  def runIntoSimple[Res](
      sql: SqlStr,
      dbTypes: DbType[Res]
  ): Future[QueryResult[Res]] =
    runIntoRes[ProductKPar[Tuple1[Res]]](sql)(using ProductK.of[DbType, Tuple1[Res]](Tuple1(dbTypes)))
      .map(_.map(_.tuple.head))

  def runIntoRes[Res[_[_]]](
      sql: SqlStr
  )(using dbTypes: Res[DbType], FA: ApplicativeKC[Res], FT: TraverseKC[Res]): Future[QueryResult[Res[Id]]] =
    makePrepared(sql) { (ps: PreparedStatement) =>
      val rs = ps.executeQuery().acquired

      val indicesState: State[Int, Res[Const[Int]]] =
        dbTypes.traverseK([A] => (_: DbType[A]) => State((acc: Int) => (acc + 1, acc)))

      val indices: Res[Const[Int]] = indicesState.runA(1).value

      val rows = Seq.unfold(rs.next())(cond =>
        Option.when(cond)(
          (
            dbTypes.map2K[Const[Int], Id](indices)(
              [A] => (dbType: DbType[A], idx: Int) => dbType.get(rs, idx)
            ),
            rs.next()
          )
        )
      )

      QueryResult(rows)
    }

  def close(): Unit = ds.close()
