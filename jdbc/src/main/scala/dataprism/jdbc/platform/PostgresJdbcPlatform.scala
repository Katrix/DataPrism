package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcCodec, PostgresJdbcTypes}
import dataprism.platform.sql.implementations.PostgresPlatform
import dataprism.sql.AnsiTypes

trait PostgresJdbcPlatform extends PostgresPlatform {

  type Api <: PostgresApi

  override type Codec[A] = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override def arrayOfType[A](tpe: Type[A]): Type[Seq[A]] = PostgresJdbcTypes.arrayOf(tpe)

  override type DbArrayCompanion = SqlDbArrayCompanion
  object DbArray extends SqlDbArrayCompanion
  override val AnsiTypes: AnsiTypes[JdbcCodec] = PostgresJdbcTypes

  type Compile = SqlCompileImpl
  object Compile extends SqlCompileImpl
}
object PostgresJdbcPlatform extends PostgresJdbcPlatform {
  override type Api = PostgresApi
  object Api extends PostgresApi

  override type Impl = DefaultCompleteImpl & SqlArraysImpl
  object Impl extends DefaultCompleteImpl, SqlArraysImpl
}
