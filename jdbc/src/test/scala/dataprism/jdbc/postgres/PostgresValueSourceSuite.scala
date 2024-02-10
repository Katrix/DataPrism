package dataprism.jdbc.postgres

import cats.effect.IO
import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class PostgresValueSourceSuite
    extends PostgresFunSuite
    with PlatformValueSourceSuite[IO, JdbcCodec, PostgresJdbcPlatform] {}
