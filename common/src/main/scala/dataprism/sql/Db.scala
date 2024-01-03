package dataprism.sql

import perspective.*

trait Db[F[_], Type[_]]:
  self =>

  def run(sql: SqlStr[Type]): F[Int]

  def runIntoSimple[Res](
      sql: SqlStr[Type],
      dbTypes: Type[Res]
  ): F[QueryResult[Res]]

  def runIntoRes[Res[_[_]]](
      sql: SqlStr[Type],
      dbTypes: Res[Type]
  )(using FT: TraverseKC[Res]): F[QueryResult[Res[Id]]]

  def mapK[G[_]](f: F :~>: G): Db[G, Type] = new Db[G, Type]:
    override def run(sql: SqlStr[Type]): G[Int] = f(self.run(sql))

    override def runIntoSimple[Res](sql: SqlStr[Type], dbTypes: Type[Res]): G[QueryResult[Res]] = f(
      self.runIntoSimple(sql, dbTypes)
    )

    override def runIntoRes[Res[_[_]]](sql: SqlStr[Type], dbTypes: Res[Type])(
        using FT: TraverseKC[Res]
    ): G[QueryResult[Res[Id]]] = f(self.runIntoRes(sql, dbTypes))
