package dataprism.jdbc.mariadb

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MariaDbDbValueSuite extends MariaDbFunSuite, PlatformDbValueSuite[JdbcCodec, MariaDbJdbcPlatform] {
  override def maxParallelism: Int = 10

  override def leastGreatestBubbleNulls: Boolean = true
  
}
