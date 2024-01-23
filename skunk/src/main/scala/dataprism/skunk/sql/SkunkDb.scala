package dataprism.skunk.sql

import scala.util.NotGiven

import cats.effect.Concurrent
import cats.effect.kernel.Resource
import dataprism.sql.{CatsTransactionDb, CatsTransactionalDb, TransactionDb}
import perspective.*
import skunk.*

class SkunkDb[F[_]: Concurrent](s: Session[F]) extends SkunkBaseDb[F](s), CatsTransactionalDb[F, Codec]:

  override def transactionResource(using NotGiven[TransactionDb[F, Codec]]): Resource[F, CatsTransactionDb[F, Codec]] =
    s.transaction.map(xa => new SkunkTransactionDb(s, xa))

  def skunkMapK[G[_]: Concurrent](f: F :~>: G): SkunkDb[G] =
    SkunkDb[G](s.mapK(new cats.arrow.FunctionK[F, G] {
      override def apply[A](fa: F[A]): G[A] = f(fa)
    }))
