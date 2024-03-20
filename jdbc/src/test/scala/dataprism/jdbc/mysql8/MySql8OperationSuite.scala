package dataprism.jdbc.mysql8

import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql8OperationSuite extends MySql8FunSuite with PlatformOperationSuite[JdbcCodec, MySql8JdbcPlatform] {
  doTestDeleteUsing()
  //TODO doTestUpdateFrom()
}
