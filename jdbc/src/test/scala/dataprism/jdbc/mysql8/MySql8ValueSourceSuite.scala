package dataprism.jdbc.mysql8

import cats.effect.IO
import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql8ValueSourceSuite extends MySql8FunSuite with PlatformValueSourceSuite[IO, JdbcCodec, MySqlJdbcPlatform] {}
