package dataprism.jdbc.h2

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object H2QuerySuite extends H2FunSuite with PlatformQuerySuite[JdbcCodec, H2JdbcPlatform] {
  doTestExcept()
  doTestIntersect()
}
