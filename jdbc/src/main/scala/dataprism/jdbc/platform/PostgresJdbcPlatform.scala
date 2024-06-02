package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcCodec, PostgresJdbcTypes}
import dataprism.platform.sql.implementations.PostgresPlatform
import dataprism.sql.AnsiTypes

trait PostgresJdbcPlatform extends PostgresPlatform {

  type Api <: PostgresApi

  override type ArrayTypeArgs[A] = Nothing // PostgresJdbcTypes.ArrayMapping[A]
  override type Codec[A]         = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override protected def arrayType[A](elemType: Type[A])(
      using extraArrayTypeArgs: ArrayTypeArgs[A]
  ): Type[Seq[A]] =
    ??? // PostgresJdbcTypes.array(elemType).notNull

  override val AnsiTypes: AnsiTypes[JdbcCodec] = PostgresJdbcTypes

  type Compile = SqlCompileImpl
  object Compile extends SqlCompileImpl
}
object PostgresJdbcPlatform extends PostgresJdbcPlatform {
  override type Api = PostgresApi
  object Api extends PostgresApi

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
