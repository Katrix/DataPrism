package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class SqliteDbValueSuite extends SqliteFunSuite with PlatformDbValueSuite[IO, JdbcCodec, SqliteJdbcPlatform] {}
