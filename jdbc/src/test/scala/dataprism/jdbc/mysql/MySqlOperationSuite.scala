package dataprism.jdbc.mysql

import cats.effect.IO
import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySqlOperationSuite extends MySqlFunSuite with PlatformOperationSuite[IO, JdbcCodec, MySqlJdbcPlatform] {
  doTestDeleteUsing()
  doTestUpdateFrom()
}
