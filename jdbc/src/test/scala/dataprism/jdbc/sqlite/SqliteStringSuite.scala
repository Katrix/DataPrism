package dataprism.jdbc.sqlite

import dataprism.PlatformStringSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object SqliteStringSuite extends SqliteFunSuite, PlatformStringSuite[JdbcCodec, SqliteJdbcPlatform] {
  override def concatWsIgnoresEmptyStrings: Boolean = true
}
