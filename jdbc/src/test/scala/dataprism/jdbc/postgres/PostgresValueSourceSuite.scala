package dataprism.jdbc.postgres

import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object PostgresValueSourceSuite extends PostgresFunSuite, PlatformValueSourceSuite[JdbcCodec, PostgresJdbcPlatform] {
  doTestFullJoin()
}
