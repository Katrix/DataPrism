package dataprism.jdbc.mysql

import cats.effect.IO
import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySqlValueSourceSuite extends MySqlFunSuite with PlatformValueSourceSuite[IO, JdbcCodec, MySqlJdbcPlatform] {}
