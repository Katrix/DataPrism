package dataprism.skunk.testsuites

import dataprism.PlatformSaneMathSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import dataprism.skunk.sql.SkunkAnsiTypes
import skunk.Codec

object PostgresMathSuite extends PostgresFunSuite, PlatformSaneMathSuite[Codec, PostgresSkunkPlatform] {
  // override def maxParallelism: Int = 10

  // override val configuredForall: PostgresDbValueSuite.PartiallyAppliedForall =
  //  forall.withConfig(CheckConfig.default.copy(perPropertyParallelism = 1))

  override protected def longCastType: SkunkAnsiTypes.TypeOf[Long]     = SkunkAnsiTypes.bigint
  override protected def doubleCastType: SkunkAnsiTypes.TypeOf[Double] = SkunkAnsiTypes.doublePrecision
}
