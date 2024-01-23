package dataprism.sql

trait TransactionDb[F[_], Codec[_]] extends Db[F, Codec]:
  type Savepoint

  def rollback: F[Unit]
  def commit: F[Unit]

  def savepoint: F[Savepoint]
  def rollback(savepoint: Savepoint): F[Unit]
