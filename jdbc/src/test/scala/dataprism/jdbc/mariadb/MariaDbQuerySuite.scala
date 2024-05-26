package dataprism.jdbc.mariadb

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MariaDbQuerySuite extends MariaDbFunSuite, PlatformQuerySuite[JdbcCodec, MariaDbJdbcPlatform] {
  doTestExcept()
  doTestIntersect()
  doTestIntersectAll()
  doTestExceptAll()
}
