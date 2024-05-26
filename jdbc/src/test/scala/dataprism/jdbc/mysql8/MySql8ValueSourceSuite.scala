package dataprism.jdbc.mysql8

import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql8ValueSourceSuite extends MySql8FunSuite, PlatformValueSourceSuite[JdbcCodec, MySql8JdbcPlatform]
