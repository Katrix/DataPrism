package dataprism.jdbc.sql

import java.sql.Connection
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.util.*

import dataprism.sql.*
import perspective.Id

trait DataSourceDb[F[_]](ds: DataSource) extends ConnectionDb[F], TransactionalDb[F, JdbcCodec]:

  protected def wrapTry[A](tryV: => Try[A]): F[A]

  override protected def getConnection(using ResourceManager): Connection = ds.getConnection.acquire

object DataSourceDb:
  def ofFuture(ds: DataSource)(using ExecutionContext): DataSourceDb[Future] = new DataSourceDb[Future](ds):
    override protected def wrapTry[A](tryV: => Try[A]): Future[A] = Future(tryV).flatMap(Future.fromTry)

    override def transaction[A](f: TransactionDb[Future, JdbcCodec] ?=> Future[A])(
        using NotGiven[TransactionDb[Future, JdbcCodec]]
    ): Future[A] =
      given man: ResourceManager.Storing = ResourceManager.Storing.make
      val (con, future) = man.addExecution {
        val con = ds.getConnection.acquire
        con.setAutoCommit(false)
        val transactionDb = new ConnectionTransactionDb(con, [B] => (tryF: () => Try[B]) => wrapTry(tryF()))
        (con, f(using transactionDb))
      }

      future.transform {
        case Failure(exception) =>
          man.addExecutionTry(con.rollback()).flatMap(_ => man.finishWithException(exception))

        case Success(v) =>
          man.addExecutionTry(con.commit()).flatMap(_ => man.finish()).map(_ => v)
      }

  def ofTrySync(ds: DataSource)(using ExecutionContext): DataSourceDb[Try] = new DataSourceDb[Try](ds):
    override protected def wrapTry[A](tryV: => Try[A]): Try[A] = tryV

    override def transaction[A](f: TransactionDb[Try, JdbcCodec] ?=> Try[A])(
        using NotGiven[TransactionDb[Try, JdbcCodec]]
    ): Try[A] =
      given man: ResourceManager.Storing = ResourceManager.Storing.make
      val (con, tryV) = man.addExecution {
        val con = ds.getConnection.acquire
        con.setAutoCommit(false)
        val transactionDb = new ConnectionTransactionDb(con, [B] => (tryF: () => Try[B]) => wrapTry(tryF()))
        (con, f(using transactionDb))
      }
      tryV match
        case Failure(exception) => man.addExecutionTry(con.rollback()).flatMap(_ => man.finishWithException(exception))
        case Success(v)         => man.addExecutionTry(con.commit()).flatMap(_ => man.finish()).map(_ => v)

  def ofIdSync(ds: DataSource)(using ExecutionContext): DataSourceDb[Id] = new DataSourceDb[Id](ds):
    override protected def wrapTry[A](tryV: => Try[A]): Id[A] = tryV.get

    override def transaction[A](f: TransactionDb[Id, JdbcCodec] ?=> Id[A])(
        using NotGiven[TransactionDb[Id, JdbcCodec]]
    ): Id[A] =
      given man: ResourceManager.Storing = ResourceManager.Storing.make
      val con                            = man.addExecution(ds.getConnection.acquire)
      con.setAutoCommit(false)

      val tryV = man.addExecutionTry {
        val transactionDb = new ConnectionTransactionDb(con, [B] => (tryF: () => Try[B]) => wrapTry(tryF()))
        f(using transactionDb)
      }
      tryV match
        case Failure(exception) =>
          man.addExecutionTry(con.rollback()).flatMap(_ => man.finishWithException(exception)).get
        case Success(v) => man.addExecutionTry(con.commit()).flatMap(_ => man.finish()).map(_ => v).get
