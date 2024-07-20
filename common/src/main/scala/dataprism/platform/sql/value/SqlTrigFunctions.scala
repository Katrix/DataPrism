package dataprism.platform.sql.value

import dataprism.sharedast.SqlExpr

trait SqlTrigFunctions extends SqlDbValuesBase {

  type DbMath <: SqlTrigMath
  trait SqlTrigMath:
    def acos(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ACos, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
    def asin(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ASin, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
    def atan(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ATan, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
    def atan2(v1: DbValue[Double], v2: DbValue[Double]): DbValue[Double] =
      Impl.function(
        SqlExpr.FunctionName.ATan2,
        Seq(v1.asAnyDbVal, v2.asAnyDbVal),
        AnsiTypes.doublePrecision
      )
    def cos(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Cos, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
    def cot(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Cot, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
    def sin(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Sin, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
    def tan(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Tan, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
}
