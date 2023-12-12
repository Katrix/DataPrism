package dataprism.skunk.sql

import java.io.Closeable
import java.sql.{Connection, PreparedStatement}
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using

import cats.Functor
import cats.data.State
import cats.syntax.all.*
import dataprism.sql.{Db, QueryResult, SqlArg, SqlStr}
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}
import skunk.data.{Completion, Type}
import skunk.util.Origin
import skunk.{Codec, Command, Decoder, Encoder, Query, Session}

class SkunkDb[F[_]: Functor](s: Session[F]) extends Db[F, Codec]:

  private def makeCommand(sql: SqlStr[Codec]): (Command[Seq[Any]], Seq[Any]) =
    val sqlStr = sql
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
      sql.args.map(_.value)
    )

  private def makeQuery[Res[_[_]]](
      sql: SqlStr[Codec],
      dbTypes: Res[Codec]
  )(using FA: ApplyKC[Res], FT: TraverseKC[Res]): (Query[Seq[Any], Res[Id]], Seq[Any]) =
    val sqlStr = sql
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
      sql.args.map(_.value)
    )

  override def run(sql: SqlStr[Codec]): F[Int] = {
    val (command, args) = makeCommand(sql)
    s.execute(command)(args).map {
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
  }

  override def runIntoSimple[Res](
      sql: SqlStr[Codec],
      dbTypes: Codec[Res]
  ): F[QueryResult[Res]] =
    runIntoRes[ProductKPar[Tuple1[Res]]](sql, ProductK.ofScalaTuple[Codec, Tuple1[Res]](Tuple1(dbTypes)))
      .map(_.map(_.tuple.head))

  override def runIntoRes[Res[_[_]]](
      sql: SqlStr[Codec],
      dbTypes: Res[Codec]
  )(using FA: ApplyKC[Res], FT: TraverseKC[Res]): F[QueryResult[Res[Id]]] = {
    val (query, args) = makeQuery(sql, dbTypes)
    s.execute(query)(args).map(xs => QueryResult(xs))
  }
