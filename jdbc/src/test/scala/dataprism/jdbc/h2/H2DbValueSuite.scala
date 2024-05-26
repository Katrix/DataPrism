package dataprism.jdbc.h2

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object H2DbValueSuite extends H2FunSuite, PlatformDbValueSuite[JdbcCodec, H2JdbcPlatform] {
  override def leastGreatestBubbleNulls: Boolean = true
}
