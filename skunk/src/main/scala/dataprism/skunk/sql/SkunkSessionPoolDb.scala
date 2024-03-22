package dataprism.skunk.sql

import scala.util.NotGiven

import cats.effect.Concurrent
import cats.effect.kernel.Resource
import dataprism.sql.{CatsTransactionDb, CatsTransactionalDb, TransactionDb}
import perspective.*
import skunk.*

class SkunkSessionPoolDb[F[_]: Concurrent](pool: Resource[F, Session[F]], autoTransact: Boolean = true)
    extends SkunkSessionDb[F](autoTransact),
      CatsTransactionalDb[F, Codec]:

  override protected def getSession: Resource[F, Session[F]] = pool

  override def transactionResource(using NotGiven[TransactionDb[F, Codec]]): Resource[F, CatsTransactionDb[F, Codec]] =
    for
      s  <- pool
      xa <- s.transaction
    yield new SkunkTransactionDb(s, xa)

  def skunkMapK[G[_]: Concurrent](f: F :~>: G): SkunkSessionPoolDb[G] =
    val catsFunK = new cats.arrow.FunctionK[F, G] {
      override def apply[A](fa: F[A]): G[A] = f(fa)
    }
    new SkunkSessionPoolDb[G](pool.mapK(catsFunK).map(_.mapK(catsFunK)))
