package dataprism.jdbc.mariadb

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MariaDbQuerySuite extends MariaDbFunSuite with PlatformQuerySuite[JdbcCodec, MariaDbJdbcPlatform] {
  doTestExcept()
  doTestIntersect()
  doTestIntersectAll()
  doTestExceptAll()
}
