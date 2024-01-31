package dataprism.jdbc.platform

import dataprism.jdbc.sql.{JdbcCodec, PostgresJdbcTypes}
import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.sql.AnsiTypes

import scala.annotation.targetName

trait PostgresJdbcPlatform extends PostgresQueryPlatform {

  type Api = PostgresApi
  val Api: Api = new PostgresApi {}

  override type ArrayTypeArgs[A] = PostgresJdbcTypes.ArrayMapping[A]
  override type Codec[A]          = JdbcCodec[A]
  extension [A](tpe: Type[A])
    @targetName("typeName")
    override def name: String = tpe.codec.name

  override protected def arrayType[A](elemType: Type[A])(
      using extraArrayTypeArgs: PostgresJdbcTypes.ArrayMapping[A]
  ): Type[Seq[A]] = 
    PostgresJdbcTypes.array(elemType).notNull

  override def AnsiTypes: AnsiTypes[JdbcCodec] = PostgresJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object PostgresJdbcPlatform extends PostgresJdbcPlatform
