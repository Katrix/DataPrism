package dataprism.platform.sql.value

import dataprism.sharedast.SqlExpr

trait SqlTrigFunctions extends SqlDbValuesBase {

  type DbMath <: SqlTrigMath
  trait SqlTrigMath:
    def acos(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ACos, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def asin(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ASin, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def atan(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ATan, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def atan2(v1: DbValue[Double], v2: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ATan2, Seq(v1.unsafeAsAnyDbVal, v2.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def cos(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Cos, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def cot(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Cot, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def sin(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Sin, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
    def tan(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Tan, Seq(v.unsafeAsAnyDbVal), AnsiTypes.doublePrecision)
}
