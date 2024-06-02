package dataprism.skunk.testsuites

import dataprism.PlatformDbValueSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import dataprism.skunk.sql.SkunkTypes
import org.scalacheck.Gen
import skunk.Codec

object PostgresDbValueSuite extends PostgresFunSuite, PlatformDbValueSuite[Codec, PostgresSkunkPlatform] {
  // override def maxParallelism: Int = 10

  // override val configuredForall: PostgresDbValueSuite.PartiallyAppliedForall =
  //  forall.withConfig(CheckConfig.default.copy(perPropertyParallelism = 1))

  override def leastGreatestBubbleNulls: Boolean = false

  import spire.implicits.*
  testBitwiseOps[Long, perspective.Id](platform, platform.AnsiTypes.bigint, Gen.choose(-10000, 10000), _.toInt)
  testBitwiseOps[Long, Option](
    platform,
    platform.AnsiTypes.bigint.nullable,
    Gen.option(Gen.choose(-10000, 10000)),
    _.toInt
  )

  testBitwiseOps[Short, perspective.Id](platform, platform.AnsiTypes.smallint, Gen.choose(-10000, 10000), _.toInt)
  testBitwiseOps[Short, Option](
    platform,
    platform.AnsiTypes.smallint.nullable,
    Gen.option(Gen.choose(-10000, 10000)),
    _.toInt
  )
}
