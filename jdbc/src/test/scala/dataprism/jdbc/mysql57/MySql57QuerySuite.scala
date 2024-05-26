package dataprism.jdbc.mysql57

import dataprism.PlatformQuerySuite
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql57QuerySuite extends MySql57FunSuite, PlatformQuerySuite[JdbcCodec, MySql57JdbcPlatform]
