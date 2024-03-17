package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class SqliteValueSourceSuite
    extends SqliteFunSuite
    with PlatformValueSourceSuite[IO, JdbcCodec, SqliteJdbcPlatform] {}
