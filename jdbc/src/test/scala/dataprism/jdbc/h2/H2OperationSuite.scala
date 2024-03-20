package dataprism.jdbc.h2

import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object H2OperationSuite extends H2FunSuite with PlatformOperationSuite[JdbcCodec, H2JdbcPlatform]
