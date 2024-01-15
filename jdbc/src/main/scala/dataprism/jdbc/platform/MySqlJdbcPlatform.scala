package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcCodec, MySqlJdbcTypeCastable, MySqlJdbcTypes}
import dataprism.platform.implementations.MySqlQueryPlatform
import dataprism.sql.AnsiTypes

trait MySqlJdbcPlatform extends MySqlQueryPlatform {
  override type Codec[A]    = JdbcCodec[A]
  override type CastType[A] = MySqlJdbcTypeCastable[A]

  extension [A](t: CastType[A])
    @targetName("castTypeName") override def castTypeName: String  = t.name
    @targetName("castTypeType") override def castTypeType: Type[A] = t.tpe.notNull

  extension [A](tpe: Type[A])
    @targetName("typeName")
    override def name: String = tpe.codec.name

  override def AnsiTypes: AnsiTypes[JdbcCodec] = MySqlJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object MySqlJdbcPlatform extends MySqlJdbcPlatform
