package dataprism.jdbc.platform.implementations

import dataprism.jdbc.sql.{JdbcType, PostgresJdbcTypes}
import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.sql.AnsiTypes

import scala.annotation.targetName

trait PostgresJdbcPlatform extends PostgresQueryPlatform {

  override type ArrayTypeArgs[A] = PostgresJdbcTypes.ArrayMapping[A]
  override type Type[A]          = JdbcType[A]
  extension [A](tpe: JdbcType[A])
    @targetName("typeName")
    override def name: String = tpe.name

  override protected def arrayType[A](elemType: JdbcType[A])(
      using extraArrayTypeArgs: PostgresJdbcTypes.ArrayMapping[A]
  ): JdbcType[Seq[A]] = PostgresJdbcTypes.array(elemType)

  override def AnsiTypes: AnsiTypes[JdbcType] = PostgresJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object PostgresJdbcPlatform extends PostgresJdbcPlatform
