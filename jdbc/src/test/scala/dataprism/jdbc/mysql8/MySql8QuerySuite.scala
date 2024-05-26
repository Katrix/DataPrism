package dataprism.jdbc.mysql8

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql8QuerySuite extends MySql8FunSuite, PlatformQuerySuite[JdbcCodec, MySql8JdbcPlatform] {
  doTestFlatmapLateral()
  doTestExcept()
  doTestIntersect()
  doTestIntersectAll()
  doTestExceptAll()
}
