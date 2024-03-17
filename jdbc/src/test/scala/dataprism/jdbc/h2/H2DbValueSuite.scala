package dataprism.jdbc.h2

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class H2DbValueSuite extends H2FunSuite with PlatformDbValueSuite[IO, JdbcCodec, H2JdbcPlatform] {
  
}
