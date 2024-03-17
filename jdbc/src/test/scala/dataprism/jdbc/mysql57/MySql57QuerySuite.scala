package dataprism.jdbc.mysql57

import cats.effect.IO
import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql57QuerySuite extends MySql57FunSuite with PlatformQuerySuite[IO, JdbcCodec, MySqlJdbcPlatform] {
  doTestFlatmapLateral()
}
