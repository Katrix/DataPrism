package dataprism.sql

trait CatsTransactionDb[F[_], Codec[_]] extends CatsDb[F, Codec], TransactionDb[F, Codec]
