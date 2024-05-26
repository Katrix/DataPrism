package dataprism.jdbc.sqlite

import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object SqliteValueSourceSuite extends SqliteFunSuite, PlatformValueSourceSuite[JdbcCodec, SqliteJdbcPlatform] {
  doTestFullJoin()
}
