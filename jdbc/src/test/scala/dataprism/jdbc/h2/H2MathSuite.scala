package dataprism.jdbc.h2

import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql
import dataprism.jdbc.sql.{JdbcAnsiTypes, JdbcCodec}
import dataprism.{PlatformSaneMathSuite, jdbc}
import spire.math.Real

object H2MathSuite extends H2FunSuite, PlatformSaneMathSuite[JdbcCodec, H2JdbcPlatform] {
  override protected def longCastType: JdbcAnsiTypes.TypeOf[Long]     = JdbcAnsiTypes.bigint
  override protected def doubleCastType: JdbcAnsiTypes.TypeOf[Double] = JdbcAnsiTypes.doublePrecision

  testTrigFunctions(platform)

  functionTest1("sinh", doubleTypeInfo, platform.DbMath.sinh, Math.sinh)
  functionTest1("cosh", doubleTypeInfo, platform.DbMath.cosh, Math.cosh)
  functionTest1("tanh", doubleTypeInfo, platform.DbMath.tanh, Math.tanh)
}
