package dataprism.jdbc.postgres

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object PostgresQuerySuite extends PostgresFunSuite, PlatformQuerySuite[JdbcCodec, PostgresJdbcPlatform] {
  doTestDistinctOn()
  doTestFlatmapLateral()
  doTestExcept()
  doTestIntersect()
  doTestIntersectAll()
  doTestExceptAll()
}
