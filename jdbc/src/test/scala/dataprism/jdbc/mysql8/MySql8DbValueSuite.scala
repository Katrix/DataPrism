package dataprism.jdbc.mysql8

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql8DbValueSuite extends MySql8FunSuite, PlatformDbValueSuite[JdbcCodec, MySql8JdbcPlatform] {
  override def maxParallelism: Int = 10

  override def leastGreatestBubbleNulls: Boolean = true
}
