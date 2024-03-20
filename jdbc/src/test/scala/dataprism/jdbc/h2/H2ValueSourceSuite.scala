package dataprism.jdbc.h2

import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object H2ValueSourceSuite extends H2FunSuite with PlatformValueSourceSuite[JdbcCodec, H2JdbcPlatform]
