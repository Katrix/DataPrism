package dataprism.jdbc.postgres

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object PostgresDbValueSuite extends PostgresFunSuite with PlatformDbValueSuite[JdbcCodec, PostgresJdbcPlatform] {
  override def maxParallelism: Int = 10

  override def leastGreatestBubbleNulls: Boolean = false
}
