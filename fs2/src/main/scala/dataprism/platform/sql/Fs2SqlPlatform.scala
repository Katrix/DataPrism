package dataprism.platform.sql

import scala.annotation.targetName

import dataprism.sql.Fs2Db
import perspective.Id

trait Fs2SqlPlatform { platform: SqlQueryPlatform =>

  extension [Res[_[_]]](op: ResultOperation[Res])
    @targetName("resultOperationRunStream") def runStream[F[_]](using db: Fs2Db[F, Codec]): fs2.Stream[F, Res[Id]] =
      runStreamWithChunkSize(512)

    @targetName("resultOperationRunStream") def runStreamWithChunkSize[F[_]](chunkSize: Int)(
        using db: Fs2Db[F, Codec]
    ): fs2.Stream[F, Res[Id]] =
      import op.given
      val (sql, types) = op.sqlAndTypes
      db.runIntoResStream(sql, types.mapK([Z] => (t: Type[Z]) => t.codec), chunkSize)

  override type Api <: Fs2Api
  trait Fs2Api {
    extension [Res[_[_]]](op: ResultOperation[Res])
      @targetName("resultOperationRunStream") def runStream[F[_]](using db: Fs2Db[F, Codec]): fs2.Stream[F, Res[Id]] =
        platform.runStream(op)

      @targetName("resultOperationRunStream") def runStreamWithChunkSize[F[_]](chunkSize: Int)(
        using db: Fs2Db[F, Codec]
      ): fs2.Stream[F, Res[Id]] = platform.runStreamWithChunkSize(op)(chunkSize)
  }
}
