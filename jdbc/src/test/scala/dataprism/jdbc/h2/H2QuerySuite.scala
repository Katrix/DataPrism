package dataprism.jdbc.h2

import cats.effect.IO
import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class H2QuerySuite extends H2FunSuite with PlatformQuerySuite[IO, JdbcCodec, H2JdbcPlatform]
