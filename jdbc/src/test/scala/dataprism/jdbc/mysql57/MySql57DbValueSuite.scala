package dataprism.jdbc.mysql57

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql57DbValueSuite extends MySql57FunSuite, PlatformDbValueSuite[JdbcCodec, MySql57JdbcPlatform] {
  override def maxParallelism: Int = 10

  override def leastGreatestBubbleNulls: Boolean = true
}
