package dataprism.jdbc.postgres

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.scalacheck.Gen

object PostgresDbValueSuite extends PostgresFunSuite, PlatformDbValueSuite[JdbcCodec, PostgresJdbcPlatform] {
  override def maxParallelism: Int = 10

  override def leastGreatestBubbleNulls: Boolean = false

  import spire.implicits.*
  testBitwiseOps[Long, perspective.Id](platform, platform.AnsiTypes.bigint, Gen.choose(-10000, 10000), _.toInt)
  testBitwiseOps[Long, Option](platform, platform.AnsiTypes.bigint.nullable, Gen.option(Gen.choose(-10000, 10000)), _.toInt)

  testBitwiseOps[Short, perspective.Id](platform, platform.AnsiTypes.smallint, Gen.choose(-10000, 10000), _.toInt)
  testBitwiseOps[Short, Option](platform, platform.AnsiTypes.smallint.nullable, Gen.option(Gen.choose(-10000, 10000)), _.toInt)
}
