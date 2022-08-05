package dataprism.runner

import dataprism.sql.{DbType, SqlStr}

import perspective.Id

trait SqlDbRunner {

  type Res[_]

  def executeUpdate(sql: SqlStr): Res[Int]

  def executeQuery[A[_[_]]](sql: SqlStr, dbTypes: A[DbType]): Res[A[Id]]
}
