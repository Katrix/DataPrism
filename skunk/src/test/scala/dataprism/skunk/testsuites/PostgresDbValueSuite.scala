package dataprism.skunk.testsuites

import dataprism.PlatformDbValueSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import skunk.Codec

object PostgresDbValueSuite extends PostgresFunSuite, PlatformDbValueSuite[Codec, PostgresSkunkPlatform] {
  // override def maxParallelism: Int = 10

  //override val configuredForall: PostgresDbValueSuite.PartiallyAppliedForall =
  //  forall.withConfig(CheckConfig.default.copy(perPropertyParallelism = 1))

  override def leastGreatestBubbleNulls: Boolean = false
}
