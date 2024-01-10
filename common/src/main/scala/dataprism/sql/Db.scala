package dataprism.sql

import perspective.*

trait Db[F[_], Codec[_]]:
  self =>

  def run(sql: SqlStr[Codec]): F[Int]

  def runIntoSimple[Res](
      sql: SqlStr[Codec],
      dbTypes: Codec[Res]
  ): F[QueryResult[Res]]

  def runIntoRes[Res[_[_]]](
      sql: SqlStr[Codec],
      dbTypes: Res[Codec]
  )(using FT: TraverseKC[Res]): F[QueryResult[Res[Id]]]

  def mapK[G[_]](f: F :~>: G): Db[G, Codec] = new Db[G, Codec]:
    override def run(sql: SqlStr[Codec]): G[Int] = f(self.run(sql))

    override def runIntoSimple[Res](sql: SqlStr[Codec], dbTypes: Codec[Res]): G[QueryResult[Res]] = f(
      self.runIntoSimple(sql, dbTypes)
    )

    override def runIntoRes[Res[_[_]]](sql: SqlStr[Codec], dbTypes: Res[Codec])(
        using FT: TraverseKC[Res]
    ): G[QueryResult[Res[Id]]] = f(self.runIntoRes(sql, dbTypes))
