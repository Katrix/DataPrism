package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformDbValueSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.{JdbcCodec, SqliteJdbcTypes}
import org.scalacheck.Gen

object SqliteDbValueSuite extends SqliteFunSuite, PlatformDbValueSuite[JdbcCodec, SqliteJdbcPlatform] {
  override def leastGreatestBubbleNulls: Boolean = true

  import spire.implicits.*
  testBitwiseOps[Long, perspective.Id](platform, platform.AnsiTypes.bigint, Gen.choose(-10000, 10000), _.toInt)
  testBitwiseOps[Long, SqliteJdbcPlatform.Nullable](
    platform,
    platform.AnsiTypes.bigint.nullable,
    sqlNullGen(Gen.choose(-10000, 10000)),
    _.toInt
  )

  testBitwiseOps[Byte, perspective.Id](
    platform,
    SqliteJdbcTypes.tinyint,
    Gen.choose(Byte.MinValue, Byte.MaxValue),
    _.toInt
  )
  testBitwiseOps[Byte, SqliteJdbcPlatform.Nullable](
    platform,
    SqliteJdbcTypes.tinyint.nullable,
    sqlNullGen(Gen.choose(Byte.MinValue, Byte.MaxValue)),
    _.toInt
  )
}
