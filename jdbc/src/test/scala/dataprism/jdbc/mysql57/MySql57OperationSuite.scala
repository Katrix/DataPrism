package dataprism.jdbc.mysql57

import cats.effect.IO
import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql57OperationSuite extends MySql57FunSuite with PlatformOperationSuite[IO, JdbcCodec, MySqlJdbcPlatform] {
  doTestDeleteUsing()
}
