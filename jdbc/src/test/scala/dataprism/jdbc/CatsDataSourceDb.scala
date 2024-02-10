package dataprism.jdbc

import java.sql.Connection
import javax.sql.DataSource

import scala.util.{NotGiven, Try}

import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import dataprism.jdbc.sql.{ConnectionDb, ConnectionTransactionDb, JdbcCodec}
import dataprism.sql.{ResourceManager, TransactionDb, TransactionalDb}

//Coppied from the cats module
class CatsDataSourceDb[F[_]: Sync](ds: DataSource) extends TransactionalDb[F, JdbcCodec], ConnectionDb[F]:

  override protected def getConnection(using ResourceManager): Connection = ds.getConnection.acquire

  override protected def wrapTry[A](tryV: => Try[A]): F[A] = Sync[F].blocking(tryV).flatMap(Sync[F].fromTry(_))

  override def transaction[A](f: TransactionDb[F, JdbcCodec] ?=> F[A])(
      using NotGiven[TransactionDb[F, JdbcCodec]]
  ): F[A] =
    transactionResource.use(db => f(using db))

  def transactionResource(
      using NotGiven[TransactionDb[F, JdbcCodec]]
  ): Resource[F, TransactionDb[F, JdbcCodec]] =
    val F = Sync[F]
    Resource
      .makeCase(F.delay(ResourceManager.Storing.make)):
        case (man, ExitCase.Succeeded)  => F.delay(man.finish())
        case (man, ExitCase.Errored(e)) => F.delay(man.finishWithException(e))
        case (man, ExitCase.Canceled)   => F.delay(man.finish())
      .flatMap: man =>
        Resource.makeCase(F.blocking(man.acquire(ds.getConnection)).flatTap(con => F.delay(con.setAutoCommit(false)))):
          case (con, ExitCase.Succeeded)  => F.blocking(con.commit())
          case (con, ExitCase.Errored(e)) => F.productR(F.blocking(con.rollback()))(F.raiseError(e))
          case (con, ExitCase.Canceled)   => F.blocking(con.rollback())
      .map: con =>
        new ConnectionTransactionDb(con, [B] => (tryF: () => Try[B]) => wrapTry(tryF()))
          with TransactionDb[F, JdbcCodec]
