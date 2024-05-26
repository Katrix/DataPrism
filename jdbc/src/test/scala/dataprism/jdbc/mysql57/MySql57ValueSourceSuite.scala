package dataprism.jdbc.mysql57

import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql57ValueSourceSuite extends MySql57FunSuite, PlatformValueSourceSuite[JdbcCodec, MySql57JdbcPlatform]
