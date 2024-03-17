package dataprism.jdbc.h2

import cats.effect.IO
import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class H2ValueSourceSuite extends H2FunSuite with PlatformValueSourceSuite[IO, JdbcCodec, H2JdbcPlatform] {}
