package dataprism.jdbc.mysql57

import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql57OperationSuite extends MySql57FunSuite with PlatformOperationSuite[JdbcCodec, MySql57JdbcPlatform] {
  doTestDeleteUsing()
}
