package dataprism.jdbc.sql

import java.sql.Connection

import scala.util.Try

import cats.Functor
import dataprism.sql.{ResourceManager, TransactionDb}

class ConnectionTransactionDb[F[_]: Functor](con: Connection, wrapTryV: [A] => (() => Try[A]) => F[A])
    extends ConnectionDb[F],
      TransactionDb[F, JdbcCodec] {

  override protected def wrapTry[A](tryV: => Try[A]): F[A] = wrapTryV(() => tryV)

  override protected def getConnection(using ResourceManager): Connection = con

  opaque type Savepoint = java.sql.Savepoint

  override def rollback: F[Unit] = wrapTry(Try(con.rollback()))

  override def commit: F[Unit] = wrapTry(Try(con.commit()))

  override def savepoint: F[Savepoint] = wrapTry(Try(con.setSavepoint()))

  override def rollback(savepoint: Savepoint): F[Unit] = wrapTry(Try(con.rollback(savepoint)))
}
