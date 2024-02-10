package dataprism.jdbc.postgres

import cats.effect.IO
import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class PostgresQuerySuite extends PostgresFunSuite with PlatformQuerySuite[IO, JdbcCodec, PostgresJdbcPlatform] {
  doTestFlatmapLateral()
}
