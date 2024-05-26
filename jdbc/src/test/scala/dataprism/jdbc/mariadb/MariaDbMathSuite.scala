package dataprism.jdbc.mariadb

import dataprism.PlatformMathSuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.{JdbcCodec, MySqlJdbcTypeCastable, MySqlJdbcTypes}
import org.scalacheck.Gen

object MariaDbMathSuite extends MariaDbFunSuite, PlatformMathSuite[JdbcCodec, MariaDbJdbcPlatform] {
  import platform.Api.*
  override def maxParallelism: Int = 10

  override protected type LongLikeCastType = Long
  override protected def longCastType: MySqlJdbcTypeCastable[Long]    = MySqlJdbcTypes.castType.signedInteger
  override protected def longLikeTypeInfo: TypeInfo[Long]             = longTypeInfo
  override protected def doubleToLongLikeCastType(d: Double): Long    = d.toLong
  override protected def longLikeCastTypeSqlNumeric: SqlNumeric[Long] = platform.sqlNumericLong

  override protected type DoubleLikeCastType = BigDecimal
  override protected def doubleCastType: MySqlJdbcTypeCastable[BigDecimal] = MySqlJdbcTypes.castType.decimalN(15, 9)
  override protected def doubleLikeTypeInfo: TypeInfo[BigDecimal] = TypeInfo(
    MySqlJdbcTypes.decimal,
    Gen.choose(BigDecimal(-10000), BigDecimal(10000)),
    BigDecimal(0.000000001),
    _ => false
  )
  override protected def doubleToDoubleLikeCastType(d: Double): BigDecimal    = BigDecimal.decimal(d)
  override protected def doubleLikeCastTypeSqlNumeric: SqlNumeric[BigDecimal] = platform.sqlNumericBigDecimal
}
