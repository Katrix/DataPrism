package dataprism.jdbc.sqlite

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object SqliteQuerySuite extends SqliteFunSuite with PlatformQuerySuite[JdbcCodec, SqliteJdbcPlatform] {
  doTestExcept()
  doTestIntersect()
}
