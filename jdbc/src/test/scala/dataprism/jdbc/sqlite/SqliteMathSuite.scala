package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformSaneMathSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.{JdbcAnsiTypes, JdbcCodec}

object SqliteMathSuite extends SqliteFunSuite with PlatformSaneMathSuite[JdbcCodec, SqliteJdbcPlatform] {
  override protected def longCastType: JdbcAnsiTypes.TypeOf[Long]     = JdbcAnsiTypes.bigint
  override protected def doubleCastType: JdbcAnsiTypes.TypeOf[Double] = JdbcAnsiTypes.doublePrecision
}
