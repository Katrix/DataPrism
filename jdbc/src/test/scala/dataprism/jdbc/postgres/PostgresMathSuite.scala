package dataprism.jdbc.postgres

import dataprism.PlatformSaneMathSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.{JdbcAnsiTypes, JdbcCodec}

object PostgresMathSuite extends PostgresFunSuite with PlatformSaneMathSuite[JdbcCodec, PostgresJdbcPlatform] {
  override def maxParallelism: Int = 10

  override protected def longCastType: JdbcAnsiTypes.TypeOf[Long]     = JdbcAnsiTypes.bigint
  override protected def doubleCastType: JdbcAnsiTypes.TypeOf[Double] = JdbcAnsiTypes.doublePrecision
}
