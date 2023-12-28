package dataprism.jdbc.platform.implementations

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcType, MySqlJdbcTypeCastable, MySqlJdbcTypes}
import dataprism.platform.implementations.MySqlQueryPlatform
import dataprism.sql.AnsiTypes

trait MySqlJdbcPlatform extends MySqlQueryPlatform {
  override type Type[A]     = JdbcType[A]
  override type CastType[A] = MySqlJdbcTypeCastable[A]

  extension [A](t: CastType[A])
    @targetName("castTypeName") override def castTypeName: String = t.name
    @targetName("castTypeType") override def castTypeType: Type[A] = t.tpe

  extension [A](tpe: JdbcType[A])
    @targetName("typeName")
    override def name: String = tpe.name

  override def AnsiTypes: AnsiTypes[JdbcType] = MySqlJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object MySqlJdbcPlatform extends MySqlJdbcPlatform
