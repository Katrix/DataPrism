package dataprism.jdbc.h2

import cats.effect.IO
import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class H2OperationSuite extends H2FunSuite with PlatformOperationSuite[IO, JdbcCodec, H2JdbcPlatform]
