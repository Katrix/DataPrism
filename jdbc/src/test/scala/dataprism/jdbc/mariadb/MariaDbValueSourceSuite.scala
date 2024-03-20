package dataprism.jdbc.mariadb

import dataprism.PlatformValueSourceSuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MariaDbValueSourceSuite extends MariaDbFunSuite with PlatformValueSourceSuite[JdbcCodec, MariaDbJdbcPlatform]
