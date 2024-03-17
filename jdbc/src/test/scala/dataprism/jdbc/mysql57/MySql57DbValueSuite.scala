package dataprism.jdbc.mysql57

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql57DbValueSuite extends MySql57FunSuite with PlatformDbValueSuite[IO, JdbcCodec, MySqlJdbcPlatform] {}
