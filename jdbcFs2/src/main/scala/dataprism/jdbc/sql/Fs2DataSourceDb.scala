package dataprism.jdbc.sql

import java.sql.SQLException
import javax.sql.DataSource

import cats.data.{State, ValidatedNel}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import dataprism.sql.{Fs2Db, ResourceManager, SqlStr}
import perspective.*

class Fs2DataSourceDb[F[_]: Sync](ds: DataSource) extends CatsDataSourceDb[F](ds), Fs2Db[F, JdbcCodec]:

  override def runIntoResStream[Res[_[_]]](sql: SqlStr[JdbcCodec], dbTypes: Res[JdbcCodec], chunkSize: Int)(
      using FT: TraverseKC[Res]
  ): fs2.Stream[F, Res[Id]] =
    val F = Sync[F]

    val indicesState: State[Int, Res[Tuple2K[JdbcCodec, Const[Int]]]] =
      dbTypes.traverseK([A] => (tpe: JdbcCodec[A]) => State((acc: Int) => (acc + 1, (tpe, acc))))

    val indices: Res[Tuple2K[JdbcCodec, Const[Int]]] = indicesState.runA(1).value

    val connectionResource = Resource
      .makeCase(F.delay(ResourceManager.Storing.make)):
        case (man, ExitCase.Succeeded)  => F.delay(man.finish())
        case (man, ExitCase.Errored(e)) => F.delay(man.finishWithException(e))
        case (man, ExitCase.Canceled)   => F.delay(man.finish())
      .flatMap: man =>
        Resource.makeCase(F.blocking(man.acquire(ds.getConnection) -> man)):
          case ((con, _), ExitCase.Succeeded)  => F.blocking(con.commit())
          case ((con, _), ExitCase.Errored(e)) => F.blocking(con.rollback()) *> F.raiseError(e)
          case ((con, _), ExitCase.Canceled)   => F.blocking(con.rollback())

    // Based a bit on how doobie does this
    fs2.Stream
      .resource(connectionResource)
      .evalMap((con, man) => F.blocking((con, makePrepared(sql, con)(using man), man)))
      .evalTap((_, ps, _) => F.blocking(ps.setFetchSize(chunkSize)))
      .flatMap { (con, ps, man) =>
        sql.args.map(_.batchSize).distinct match
          case Seq()          => fs2.Stream((con, ps, man, None))
          case Seq(batchSize) => fs2.Stream.emits((0 until batchSize).map(batch => (con, ps, man, Some(batch))))
          case sizes => fs2.Stream.raiseError(new SQLException(s"Multiple batch sizes: ${sizes.mkString(" ")}"))
      }
      .flatMap { (con, ps, man, batch) =>
        given ResourceManager = man

        val readChunk: F[Seq[Res[Id]]] = Sync[F]
          .blocking {
            batch.foreach(b => setArgs(con, ps, sql.args, b))
            val rs = man.acquire(ps.executeQuery())

            Seq
              .unfold((rs.next(), 0))((cond, read) =>
                Option.when(cond && read < chunkSize) {
                  (
                    indices.traverseK[[A] =>> ValidatedNel[String, A], Id](
                      [A] => (t: (JdbcCodec[A], Int)) => t._1.get(rs, t._2, con).toValidatedNel
                    ),
                    (rs.next(), read + 1)
                  )
                }
              )
              .sequence
              .leftMap(es => new SQLException(s"Failed to map columns: ${es.toList.mkString(", ")}"))
          }
          .flatMap(v => F.fromValidated(v))

        fs2.Stream.repeatEval(readChunk).takeWhile(_.nonEmpty).flatMap(fs2.Stream.emits)
      }
