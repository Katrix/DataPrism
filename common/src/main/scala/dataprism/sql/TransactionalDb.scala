package dataprism.sql

import scala.util.NotGiven

trait TransactionalDb[F[_], Codec[_]] extends Db[F, Codec]:

  def transaction[A](f: TransactionDb[F, Codec] ?=> F[A])(using NotGiven[TransactionDb[F, Codec]]): F[A]
