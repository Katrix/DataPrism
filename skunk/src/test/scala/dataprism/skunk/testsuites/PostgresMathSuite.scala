package dataprism.skunk.testsuites

import dataprism.PlatformSaneMathSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import dataprism.skunk.sql.SkunkAnsiTypes
import org.scalacheck.Gen
import skunk.Codec
import spire.math.Real

object PostgresMathSuite extends PostgresFunSuite, PlatformSaneMathSuite[Codec, PostgresSkunkPlatform] {
  // override def maxParallelism: Int = 10

  // override val configuredForall: PostgresDbValueSuite.PartiallyAppliedForall =
  //  forall.withConfig(CheckConfig.default.copy(perPropertyParallelism = 1))

  override protected def longCastType: SkunkAnsiTypes.TypeOf[Long]     = SkunkAnsiTypes.bigint
  override protected def doubleCastType: SkunkAnsiTypes.TypeOf[Double] = SkunkAnsiTypes.doublePrecision

  testTrigFunctions(platform)

  functionTest1("sinh", doubleTypeInfo, platform.DbMath.sinh, Math.sinh)
  functionTest1("cosh", doubleTypeInfo, platform.DbMath.cosh, Math.cosh)
  functionTest1("tanh", doubleTypeInfo, platform.DbMath.tanh, Math.tanh)
  functionTest1("asinh", doubleTypeInfo, platform.DbMath.asinh, a => Real.asinh(Real(a)).toDouble)
  functionTest1("acosh", doubleTypeInfo.copy(gen = Gen.choose(1D, 10000D)), platform.DbMath.acosh, a => Real.acosh(Real(a)).toDouble)
  functionTest1("atanh", doubleTypeInfo.copy(gen = Gen.choose(-1D, 1D)), platform.DbMath.atanh, a => Real.atanh(Real(a)).toDouble)
}
