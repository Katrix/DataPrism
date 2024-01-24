package dataprism.sql

import perspective.{Id, TraverseKC}

trait Fs2Db[F[_], Codec[_]] extends CatsDb[F, Codec]:
  
  def runIntoResStream[Res[_[_]]](
      sql: SqlStr[Codec],
      dbTypes: Res[Codec],
      chunkSize: Int
  )(using FT: TraverseKC[Res]): fs2.Stream[F, Res[Id]]
