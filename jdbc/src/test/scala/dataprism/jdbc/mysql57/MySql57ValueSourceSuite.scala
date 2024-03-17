package dataprism.jdbc.mysql57

import cats.effect.IO
import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql57ValueSourceSuite extends MySql57FunSuite with PlatformValueSourceSuite[IO, JdbcCodec, MySqlJdbcPlatform] {}
