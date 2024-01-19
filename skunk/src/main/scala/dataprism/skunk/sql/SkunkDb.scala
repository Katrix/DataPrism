package dataprism.skunk.sql

import cats.data.State
import cats.effect.Concurrent
import cats.syntax.all.*
import dataprism.sql.{Db, SqlStr}
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}
import skunk.*
import skunk.data.{Completion, Type}
import skunk.util.Origin

import java.sql.SQLException

class SkunkDb[F[_]: Concurrent](s: Session[F]) extends Db[F, Codec]:

  private def makeCommand(sql: SqlStr[Codec]): (Command[Seq[Any]], Seq[Seq[Any]]) =
    val sqlStr     = sql
    val batchSizes = sql.args.map(_.batchSize).distinct
    if batchSizes.length != 1 then throw new SQLException(s"Multiple batch sizes: ${batchSizes.mkString(" ")}")
    val batchSize = batchSizes.head

    (
      Command(
        sql.str,
        Origin.unknown,
        new Encoder[Seq[Any]] {
          override def sql: State[Int, String] = State.pure("")

          override def encode(a: Seq[Any]): List[Option[String]] =
            sqlStr.args.map(_.tpe).zip(a).toList.flatMap(t => t._1.asInstanceOf[Codec[Any]].encode(t._2))

          override def types: List[Type] = sqlStr.args.toList.flatMap(_.tpe.types)
        }
      ),
      Seq.tabulate(batchSize)(batch => sql.args.map(_.value(batch)))
    )

  private def makeQuery[Res[_[_]]](
      sql: SqlStr[Codec],
      dbTypes: Res[Codec]
  )(using FT: TraverseKC[Res]): (Query[Seq[Any], Res[Id]], Seq[Seq[Any]]) =
    val sqlStr     = sql
    val batchSizes = sql.args.map(_.batchSize).distinct
    if batchSizes.length != 1 then throw new SQLException(s"Multiple batch sizes: ${batchSizes.mkString(" ")}")
    val batchSize = batchSizes.head

    (
      Query(
        sql.str,
        Origin.unknown,
        new Encoder[Seq[Any]] {
          override def sql: State[Int, String] = State.pure("")

          override def encode(a: Seq[Any]): List[Option[String]] =
            sqlStr.args.map(_.tpe).zip(a).toList.flatMap(t => t._1.asInstanceOf[Codec[Any]].encode(t._2))

          override def types: List[Type] = sqlStr.args.toList.flatMap(_.tpe.types)
        },
        new Decoder[Res[Id]] {
          override def types: List[Type] = dbTypes.foldMapK([Z] => (codec: Codec[Z]) => codec.types)

          override def decode(offset: Int, ss: List[Option[String]]): Either[Decoder.Error, Res[Id]] = {
            val indicesState: State[Int, Res[[Z] =>> Either[Decoder.Error, Z]]] =
              dbTypes.traverseK([A] => (c: Codec[A]) => State((acc: Int) => (acc + c.types.length, c.decode(acc, ss))))

            indicesState.runA(offset).value.sequenceIdK
          }
        }
      ),
      Seq.tabulate(batchSize)(batch => sql.args.map(_.value(batch)))
    )

  override def run(sql: SqlStr[Codec]): F[Int] = {
    val (command, batchArgs) = makeCommand(sql)
    s.prepare(command)
      .flatMap(
        _.pipe
          .apply(fs2.Stream(batchArgs*))
          .map {
            case Completion.Begin                   => 0
            case Completion.Commit                  => 0
            case Completion.CreateIndex             => 0
            case Completion.Delete(count)           => count
            case Completion.DropIndex               => 0
            case Completion.Listen                  => 0
            case Completion.LockTable               => 0
            case Completion.Notify                  => 0
            case Completion.Reset                   => 0
            case Completion.Rollback                => 0
            case Completion.Savepoint               => 0
            case Completion.Select(count)           => count
            case Completion.Set                     => 0
            case Completion.Truncate                => 0
            case Completion.Unlisten                => 0
            case Completion.Update(count)           => count
            case Completion.Insert(count)           => count
            case Completion.CreateTable             => 0
            case Completion.DropTable               => 0
            case Completion.AlterTable              => 0
            case Completion.CreateSchema            => 0
            case Completion.DropSchema              => 0
            case Completion.CreateType              => 0
            case Completion.DropType                => 0
            case Completion.AlterType               => 0
            case Completion.CreateFunction          => 0
            case Completion.DropFunction            => 0
            case Completion.Copy(count)             => count
            case Completion.Show                    => 0
            case Completion.Do                      => 0
            case Completion.CreateView              => 0
            case Completion.DropView                => 0
            case Completion.CreateProcedure         => 0
            case Completion.DropProcedure           => 0
            case Completion.Call                    => 0
            case Completion.CreateDomain            => 0
            case Completion.DropDomain              => 0
            case Completion.CreateSequence          => 0
            case Completion.AlterSequence           => 0
            case Completion.DropSequence            => 0
            case Completion.CreateDatabase          => 0
            case Completion.DropDatabase            => 0
            case Completion.CreateRole              => 0
            case Completion.DropRole                => 0
            case Completion.CreateMaterializedView  => 0
            case Completion.RefreshMaterializedView => 0
            case Completion.DropMaterializedView    => 0
            case Completion.CreateExtension         => 0
            case Completion.DropExtension           => 0
            case Completion.CreateTrigger           => 0
            case Completion.AlterTrigger            => 0
            case Completion.DropTrigger             => 0
            case Completion.SetConstraints          => 0
            case Completion.Explain                 => 0
            case Completion.Grant                   => 0
            case Completion.Revoke                  => 0
            case Completion.AlterIndex              => 0
            case Completion.Unknown(_)              => 0
          }
          .compile
          .fold(0)(_ + _)
      )
  }

  override def runIntoSimple[Res](
      sql: SqlStr[Codec],
      dbTypes: Codec[Res]
  ): F[Seq[Res]] =
    runIntoRes[ProductKPar[Tuple1[Res]]](sql, ProductK.ofScalaTuple[Codec, Tuple1[Res]](Tuple1(dbTypes)))
      .map(_.map(_.tuple.head))

  override def runIntoRes[Res[_[_]]](
      sql: SqlStr[Codec],
      dbTypes: Res[Codec],
      minRows: Int = 0,
      maxRows: Int = -1
  )(using FT: TraverseKC[Res]): F[Seq[Res[Id]]] = {
    //TODO: Limit based on min and max rows
    val (query, batchArgs) = makeQuery(sql, dbTypes)
    s.prepare(query).flatMap(q => q.pipe(512).apply(fs2.Stream.apply(batchArgs*)).compile.toList.map(_.toSeq))
  }

  def skunkMapK[G[_]: Concurrent](f: F :~>: G): SkunkDb[G] =
    SkunkDb[G](s.mapK(new cats.arrow.FunctionK[F, G] {
      override def apply[A](fa: F[A]): G[A] = f(fa)
    }))
