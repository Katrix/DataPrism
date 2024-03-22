package dataprism.jdbc.platform

import dataprism.jdbc.sql.{JdbcCodec, PostgresJdbcTypes}
import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.sql.AnsiTypes

import scala.annotation.targetName

trait PostgresJdbcPlatform extends PostgresQueryPlatform {

  type Api <: PostgresApi

  override type ArrayTypeArgs[A] = Nothing // PostgresJdbcTypes.ArrayMapping[A]
  override type Codec[A]          = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override protected def arrayType[A](elemType: Type[A])(
      using extraArrayTypeArgs: ArrayTypeArgs[A]
  ): Type[Seq[A]] = 
    ??? // PostgresJdbcTypes.array(elemType).notNull

  override val AnsiTypes: AnsiTypes[JdbcCodec] = PostgresJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object PostgresJdbcPlatform extends PostgresJdbcPlatform {
  override type Api = PostgresApi
  object Api extends PostgresApi
}
