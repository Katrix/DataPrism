package dataprism.platform.sql.value

import scala.util.NotGiven

import dataprism.sharedast.SqlExpr
import dataprism.sql.SqlNull

trait SqlSimpleMath extends SqlDbValuesBase {

  type DbMath <: SimpleSqlDbMath
  trait SimpleSqlDbMath:
    def pow[A: SqlNumeric](a: DbValue[A], b: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Pow, Seq(a.asAnyDbVal, b.asAnyDbVal), b.tpe)

    def sqrt[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Sqrt, Seq(a.asAnyDbVal), a.tpe)

    def abs[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Abs, Seq(a.asAnyDbVal), a.tpe)

    def ceil[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Ceiling, Seq(a.asAnyDbVal), a.tpe)

    def floor[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Floor, Seq(a.asAnyDbVal), a.tpe)

    def toDegrees[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Degrees, Seq(a.asAnyDbVal), a.tpe)

    def toRadians[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Radians, Seq(a.asAnyDbVal), a.tpe)

    def log[A: SqlFractional](a: DbValue[A], b: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Log, Seq(a.asAnyDbVal, b.asAnyDbVal), b.tpe)

    def ln[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Ln, Seq(a.asAnyDbVal), a.tpe)

    def log10[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Log10, Seq(a.asAnyDbVal), a.tpe)

    def log2[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Log2, Seq(a.asAnyDbVal), a.tpe)

    def exp[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Exp, Seq(a.asAnyDbVal), a.tpe)

    def sign[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Sign, Seq(a.asAnyDbVal), a.tpe)

    def pi[A: SqlNumeric](tpe: CastType[A])(using NotGiven[SqlNull <:< A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Pi, Nil, tpe.castTypeType).cast(tpe)

    def random[A: SqlNumeric](tpe: CastType[A])(using NotGiven[SqlNull <:< A]): DbValue[A] =
      Impl.function(SqlExpr.FunctionName.Random, Nil, tpe.castTypeType).cast(tpe)
}
