package dataprism.jdbc.mysql

import cats.effect.IO
import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySqlQuerySuite extends MySqlFunSuite with PlatformQuerySuite[IO, JdbcCodec, MySqlJdbcPlatform] {
  doTestFlatmapLateral()
}
