package dataprism.jdbc.mysql8

import cats.effect.IO
import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql8OperationSuite extends MySql8FunSuite with PlatformOperationSuite[IO, JdbcCodec, MySqlJdbcPlatform] {
  doTestDeleteUsing()
}
