package dataprism.jdbc.postgres

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class PostgresDbValueSuite extends PostgresFunSuite with PlatformDbValueSuite[IO, JdbcCodec, PostgresJdbcPlatform] {}
