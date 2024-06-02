package dataprism.jdbc.sqlite

import dataprism.PlatformSaneMathSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.{JdbcAnsiTypes, JdbcCodec}
import org.scalacheck.Gen
import spire.math.Real

object SqliteMathSuite extends SqliteFunSuite, PlatformSaneMathSuite[JdbcCodec, SqliteJdbcPlatform] {
  override protected def longCastType: JdbcAnsiTypes.TypeOf[Long]     = JdbcAnsiTypes.bigint
  override protected def doubleCastType: JdbcAnsiTypes.TypeOf[Double] = JdbcAnsiTypes.doublePrecision

  testTrigFunctions(platform)

  functionTest1("sinh", doubleTypeInfo, platform.DbMath.sinh, Math.sinh)
  functionTest1("cosh", doubleTypeInfo, platform.DbMath.cosh, Math.cosh)
  functionTest1("tanh", doubleTypeInfo, platform.DbMath.tanh, Math.tanh)
  functionTest1("asinh", doubleTypeInfo, platform.DbMath.asinh, a => Real.asinh(Real(a)).toDouble)
  functionTest1(
    "acosh",
    doubleTypeInfo.copy(gen = Gen.choose(1D, 10000D)),
    platform.DbMath.acosh,
    a => Real.acosh(Real(a)).toDouble
  )
  functionTest1(
    "atanh",
    doubleTypeInfo.copy(gen = Gen.choose(-1D, 1D)),
    platform.DbMath.atanh,
    a => Real.atanh(Real(a)).toDouble
  )
}
