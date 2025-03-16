package dataprism.jdbc.h2

import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.{H2JdbcTypes, JdbcCodec}
import org.scalacheck.Gen

object H2DbValueSuite extends H2FunSuite, PlatformDbValueSuite[JdbcCodec, H2JdbcPlatform] {
  override def leastGreatestBubbleNulls: Boolean = true

  import spire.implicits.*
  testBitwiseOps[Long, perspective.Id](platform, platform.AnsiTypes.bigint, Gen.choose(-10000, 10000), _.toInt)
  testBitwiseOps[Long, H2JdbcPlatform.Nullable](
    platform,
    platform.AnsiTypes.bigint.nullable,
    sqlNullGen(Gen.choose(-10000, 10000)),
    _.toInt
  )

  testBitwiseOps[Byte, perspective.Id](platform, H2JdbcTypes.tinyint, Gen.choose(Byte.MinValue, Byte.MaxValue), _.toInt)
  testBitwiseOps[Byte, H2JdbcPlatform.Nullable](
    platform,
    H2JdbcTypes.tinyint.nullable,
    sqlNullGen(Gen.choose(Byte.MinValue, Byte.MaxValue)),
    _.toInt
  )
}
