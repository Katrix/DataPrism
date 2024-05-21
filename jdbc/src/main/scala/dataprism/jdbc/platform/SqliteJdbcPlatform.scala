package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcCodec, SqliteJdbcTypes}
import dataprism.platform.implementations.SqliteQueryPlatform
import dataprism.sql.AnsiTypes

trait SqliteJdbcPlatform extends SqliteQueryPlatform {

  type Api <: SqliteApi

  override type Codec[A] = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override val AnsiTypes: AnsiTypes[JdbcCodec] = SqliteJdbcTypes

  type Compile = SqlCompileImpl
  object Compile extends SqlCompileImpl
}
object SqliteJdbcPlatform extends SqliteJdbcPlatform {
  override type Api = SqliteApi
  object Api extends SqliteApi

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
