package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class SqliteQuerySuite extends SqliteFunSuite with PlatformQuerySuite[IO, JdbcCodec, SqliteJdbcPlatform]
