package dataprism.jdbc.mysql8

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class MySql8DbValueSuite extends MySql8FunSuite with PlatformDbValueSuite[IO, JdbcCodec, MySqlJdbcPlatform] {}
