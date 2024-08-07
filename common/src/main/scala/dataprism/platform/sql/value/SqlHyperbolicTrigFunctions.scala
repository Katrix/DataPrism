package dataprism.platform.sql.value

import dataprism.sharedast.SqlExpr

trait SqlHyperbolicTrigFunctions extends SqlDbValuesBase {

  type DbMath <: SqlHyperbolicTrigMath

  trait ASinhCapability
  trait ACoshCapability
  trait ATanhCapability

  trait SqlHyperbolicTrigMath:
    def acosh(v: DbValue[Double])(using ACoshCapability): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ACosh, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)

    def asinh(v: DbValue[Double])(using ASinhCapability): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ASinh, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)

    def atanh(v: DbValue[Double])(using ATanhCapability): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.ATanh, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)

    def cosh(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Cosh, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)

    def sinh(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Sinh, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)

    def tanh(v: DbValue[Double]): DbValue[Double] =
      Impl.function(SqlExpr.FunctionName.Tanh, Seq(v.asAnyDbVal), AnsiTypes.doublePrecision)
}
