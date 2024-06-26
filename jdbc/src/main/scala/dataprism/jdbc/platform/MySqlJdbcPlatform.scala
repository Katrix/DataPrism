package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcCodec, MySqlJdbcTypeCastable, MySqlJdbcTypes}
import dataprism.platform.sql.implementations.MySqlPlatform
import dataprism.sql.AnsiTypes

trait MySqlJdbcPlatform extends MySqlPlatform {
  override type Codec[A]    = JdbcCodec[A]
  override type CastType[A] = MySqlJdbcTypeCastable[A]

  type Api <: MySqlApi

  extension [A](t: CastType[A])
    @targetName("castTypeName") override def castTypeName: String  = t.name
    @targetName("castTypeType") override def castTypeType: Type[A] = t.tpe.notNull

  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override val AnsiTypes: AnsiTypes[JdbcCodec] = MySqlJdbcTypes

  type Compile = SqlCompileImpl
  object Compile extends SqlCompileImpl
}
