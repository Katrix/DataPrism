package dataprism.jdbc.postgres

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object PostgresQuerySuite extends PostgresFunSuite with PlatformQuerySuite[JdbcCodec, PostgresJdbcPlatform] {
  doTestFlatmapLateral()
  doTestExcept()
  doTestIntersect()
  doTestIntersectAll()
  doTestExceptAll()
}
