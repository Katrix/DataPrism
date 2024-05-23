package dataprism.jdbc.mysql8

import dataprism.PlatformMathSuite
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.{JdbcCodec, MySqlJdbcTypeCastable, MySqlJdbcTypes}
import org.scalacheck.Gen

object MySql8MathSuite extends MySql8FunSuite with PlatformMathSuite[JdbcCodec, MySql8JdbcPlatform] {
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
