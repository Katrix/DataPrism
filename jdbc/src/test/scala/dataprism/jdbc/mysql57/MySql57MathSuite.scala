package dataprism.jdbc.mysql57

import dataprism.PlatformMathSuite
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.{JdbcCodec, MySqlJdbcTypeCastable, MySqlJdbcTypes}

object MySql57MathSuite extends MySql57FunSuite, PlatformMathSuite[JdbcCodec, MySql57JdbcPlatform] {
  import platform.Api.*
  override def maxParallelism: Int = 10

  override protected type LongLikeCastType = Long
  override protected def longCastType: MySqlJdbcTypeCastable[Long]    = MySqlJdbcTypes.castType.signedInteger
  override protected def longLikeTypeInfo: TypeInfo[Long]             = longTypeInfo
  override protected def doubleToLongLikeCastType(d: Double): Long    = d.toLong
  override protected def longLikeCastTypeSqlNumeric: SqlNumeric[Long] = platform.sqlNumericLong

  override protected type DoubleLikeCastType = BigDecimal
  override protected def doubleCastType: MySqlJdbcTypeCastable[BigDecimal]    = MySqlJdbcTypes.castType.decimalN(15, 9)
  override protected def doubleLikeTypeInfo: TypeInfo[BigDecimal]             = decimalTypeInfo
  override protected def doubleToDoubleLikeCastType(d: Double): BigDecimal    = BigDecimal.decimal(d)
  override protected def doubleLikeCastTypeSqlNumeric: SqlNumeric[BigDecimal] = platform.sqlNumericBigDecimal

  testTrigFunctions(platform)
}
