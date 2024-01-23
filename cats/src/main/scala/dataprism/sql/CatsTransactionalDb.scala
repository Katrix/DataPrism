package dataprism.sql
import scala.util.NotGiven

import cats.effect.kernel.{MonadCancelThrow, Resource}

trait CatsTransactionalDb[F[_]: MonadCancelThrow, Codec[_]] extends CatsDb[F, Codec], TransactionalDb[F, Codec]:

  override def transaction[A](f: TransactionDb[F, Codec] ?=> F[A])(using NotGiven[TransactionDb[F, Codec]]): F[A] =
    transactionResource.use(db => f(using db))

  def transactionResource(using NotGiven[TransactionDb[F, Codec]]): Resource[F, CatsTransactionDb[F, Codec]]
