package dataprism.jdbc.mariadb

import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MariaDbOperationSuite extends MariaDbFunSuite with PlatformOperationSuite[JdbcCodec, MariaDbJdbcPlatform] {
  doTestDeleteUsing()
  //TODO doTestUpdateFrom()

  doTestInsertReturning()
}
