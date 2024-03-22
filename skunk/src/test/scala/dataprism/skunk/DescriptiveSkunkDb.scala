package dataprism.skunk

import cats.effect.Concurrent
import cats.effect.kernel.Resource
import cats.syntax.all.*
import dataprism.skunk.sql.SkunkSessionPoolDb
import dataprism.sql.SqlStr
import perspective.{Id, TraverseKC}
import skunk.{Codec, Session}

class DescriptiveSkunkDb[F[_]: Concurrent](s: Resource[F, Session[F]]) extends SkunkSessionPoolDb[F](s):

  override def runBatch(sql: SqlStr[Codec]): F[Seq[Int]] = super.runBatch(sql).adaptError { case e =>
    new DescriptiveSqlException(sql.str, e)
  }

  override def run(sql: SqlStr[Codec]): F[Int] = super.run(sql).adaptError { case e =>
    new DescriptiveSqlException(sql.str, e)
  }

  override def runIntoSimple[Res](sql: SqlStr[Codec], dbTypes: Codec[Res]): F[Seq[Res]] =
    super.runIntoSimple(sql, dbTypes).adaptError { case e =>
      new DescriptiveSqlException(sql.str, e)
    }

  override def runIntoRes[Res[_[_]]](sql: SqlStr[Codec], dbTypes: Res[Codec], minRows: Int, maxRows: Int)(
      using FT: TraverseKC[Res]
  ): F[Seq[Res[Id]]] = super.runIntoRes(sql, dbTypes, minRows, maxRows).adaptError { case e =>
    new DescriptiveSqlException(sql.str, e)
  }
