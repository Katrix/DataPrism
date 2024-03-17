package dataprism.jdbc.mysql8

import cats.effect.IO
import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql8QuerySuite extends MySql8FunSuite with PlatformQuerySuite[IO, JdbcCodec, MySqlJdbcPlatform] {
  doTestFlatmapLateral()
}
