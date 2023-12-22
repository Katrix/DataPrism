package dataprism.jdbc.platform.implementations

import dataprism.jdbc.sql.{JdbcType, MySqlJdbcTypes, PostgresJdbcTypes}
import dataprism.platform.implementations.MySqlQueryPlatform
import dataprism.sql.AnsiTypes

import scala.annotation.targetName

trait MySqlJdbcPlatform extends MySqlQueryPlatform {
  override type Type[A] = JdbcType[A]
  extension [A](tpe: JdbcType[A])
    @targetName("typeName")
    override def name: String = tpe.name

  override def AnsiTypes: AnsiTypes[JdbcType] = MySqlJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object MySqlJdbcPlatform extends MySqlJdbcPlatform
