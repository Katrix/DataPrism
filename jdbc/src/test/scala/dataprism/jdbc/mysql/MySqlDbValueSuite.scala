package dataprism.jdbc.mysql

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySqlDbValueSuite extends MySqlFunSuite with PlatformDbValueSuite[IO, JdbcCodec, MySqlJdbcPlatform] {}
