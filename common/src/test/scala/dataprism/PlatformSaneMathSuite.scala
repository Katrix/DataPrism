package dataprism

import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.SelectedType

trait PlatformSaneMathSuite[Codec0[
    _
], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A]; type CastType[A] = SelectedType[Codec0, A] }]
    extends PlatformMathSuite[Codec0, Platform]:
  import platform.AnsiTypes
  import platform.Api.*

  override protected type LongLikeCastType = Long
  override protected def longCastType: AnsiTypes.TypeOf[Long]         = AnsiTypes.bigint
  override protected def longLikeTypeInfo: TypeInfo[Long]             = longTypeInfo
  override protected def longLikeCastTypeSqlNumeric: SqlNumeric[Long] = platform.sqlNumericLong
  override protected def doubleToLongLikeCastType(d: Double): Long    = d.toLong

  override protected type DoubleLikeCastType = Double
  override protected def doubleCastType: AnsiTypes.TypeOf[Double]         = AnsiTypes.doublePrecision
  override protected def doubleLikeTypeInfo: TypeInfo[Double]             = doubleTypeInfo
  override protected def doubleLikeCastTypeSqlNumeric: SqlNumeric[Double] = platform.sqlNumericDouble
  override protected def doubleToDoubleLikeCastType(d: Double): Double    = d
