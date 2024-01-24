package dataprism.skunk.sql

import scala.util.NotGiven

import cats.effect.Concurrent
import cats.effect.kernel.Resource
import dataprism.sql.{CatsTransactionDb, CatsTransactionalDb, Fs2Db, SqlStr, TransactionDb}
import perspective.*
import skunk.*

class SkunkDb[F[_]: Concurrent](s: Session[F])
    extends SkunkBaseDb[F](s),
      CatsTransactionalDb[F, Codec],
      Fs2Db[F, Codec]:

  override def transactionResource(using NotGiven[TransactionDb[F, Codec]]): Resource[F, CatsTransactionDb[F, Codec]] =
    s.transaction.map(xa => new SkunkTransactionDb(s, xa))

  override def runIntoResStream[Res[_[_]]](sql: SqlStr[Codec], dbTypes: Res[Codec], chunkSize: Int)(
      using FT: TraverseKC[Res]
  ): fs2.Stream[F, Res[Id]] =
    val (query, batchArgs) = makeQuery(sql, dbTypes)
    fs2.Stream.eval(s.prepare(query)).flatMap(q => q.pipe(512).apply(fs2.Stream.apply(batchArgs*)))

  def skunkMapK[G[_]: Concurrent](f: F :~>: G): SkunkDb[G] =
    SkunkDb[G](s.mapK(new cats.arrow.FunctionK[F, G] {
      override def apply[A](fa: F[A]): G[A] = f(fa)
    }))
