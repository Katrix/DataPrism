package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object SqliteDbValueSuite extends SqliteFunSuite, PlatformDbValueSuite[JdbcCodec, SqliteJdbcPlatform] {
  override def leastGreatestBubbleNulls: Boolean = true
}
