package dataprism.skunk.sql

import cats.effect.Concurrent
import cats.syntax.all.*
import dataprism.sql.CatsTransactionDb
import skunk.{Codec, Session, Transaction}

class SkunkTransactionDb[F[_]: Concurrent](s: Session[F], val xa: Transaction[F])
    extends SkunkBaseDb[F](s),
      CatsTransactionDb[F, Codec] {

  override type Savepoint = xa.Savepoint

  override def savepoint: F[xa.Savepoint] = xa.savepoint

  override def rollback: F[Unit] = xa.rollback.void

  override def commit: F[Unit] = xa.commit.void

  override def rollback(savepoint: xa.Savepoint): F[Unit] = xa.savepoint.void
}
