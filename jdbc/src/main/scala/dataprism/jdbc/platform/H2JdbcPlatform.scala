package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{H2JdbcTypes, JdbcCodec}
import dataprism.platform.sql.implementations.H2Platform
import dataprism.sql.AnsiTypes

trait H2JdbcPlatform extends H2Platform {

  type Api <: H2Api

  override type Codec[A] = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override def arrayOfType[A](tpe: Type[A]): Type[Seq[A]] = H2JdbcTypes.arrayOf(tpe)

  override type DbArrayCompanion = SqlDbArrayCompanion
  object DbArray extends SqlDbArrayCompanion
  override val AnsiTypes: AnsiTypes[JdbcCodec] = H2JdbcTypes

  type Compile = SqlCompileImpl
  object Compile extends SqlCompileImpl
}
object H2JdbcPlatform extends H2JdbcPlatform {
  override type Api = H2Api
  object Api extends H2Api

  override type Impl = DefaultCompleteImpl & SqlArraysImpl
  object Impl extends DefaultCompleteImpl, SqlArraysImpl
}
