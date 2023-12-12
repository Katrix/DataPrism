package dataprism.jdbc.platform.implementations

import dataprism.jdbc.sql.{JdbcType, PostgresJdbcTypes}
import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.sql.AnsiTypes

class PostgresJdbcPlatform extends PostgresQueryPlatform {

  override type ArrayTypeArgs[A] = PostgresJdbcTypes.ArrayMapping[A]
  override type Type[A]          = JdbcType[A]

  override def arrayType[A](elemType: JdbcType[A])(
      using extraArrayTypeArgs: PostgresJdbcTypes.ArrayMapping[A]
  ): JdbcType[Seq[A]] = PostgresJdbcTypes.array(elemType)

  override def ansiTypes: AnsiTypes[JdbcType] = PostgresJdbcTypes
}
