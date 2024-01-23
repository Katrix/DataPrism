package dataprism.sql

trait CatsDb[F[_], Codec[_]] extends Db[F, Codec]
