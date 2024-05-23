package dataprism.jdbc.h2

import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql
import dataprism.jdbc.sql.{JdbcAnsiTypes, JdbcCodec}
import dataprism.{PlatformSaneMathSuite, jdbc}

object H2MathSuite extends H2FunSuite with PlatformSaneMathSuite[JdbcCodec, H2JdbcPlatform] {
  override protected def longCastType: JdbcAnsiTypes.TypeOf[Long]     = JdbcAnsiTypes.bigint
  override protected def doubleCastType: JdbcAnsiTypes.TypeOf[Double] = JdbcAnsiTypes.doublePrecision
}
