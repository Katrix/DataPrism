package dataprism.platform.sql

import scala.annotation.targetName

import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.Column
import perspective.*

trait SqlQueryPlatformDbValue { platform: SqlQueryPlatform =>

  trait SqlBinOpBase {
    def ast: SqlExpr.BinaryOperation
  }

  type BinOp[LHS, RHS, R] <: SqlBinOpBase

  enum SqlBinOp[LHS, RHS, R](op: SqlExpr.BinaryOperation) extends SqlBinOpBase {
    case Eq[A]()  extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Eq)
    case Neq[A]() extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Neq)
    case And      extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolAnd)
    case Or       extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolOr)

    override def ast: SqlExpr.BinaryOperation = op
  }

  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait SqlDbValueBase[A] {

    def ast: SqlExpr

    def hasGroupBy: Boolean

    def asSqlDbVal: Option[SqlDbValue[A]]
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  enum SqlDbValue[A] extends SqlDbValueBase[A] {
    case DbColumn(column: Column[A])
    case QueryColumn(queryName: String, fromName: String)
    case GroupBy(value: DbValue[A])
    case JoinNullable[A](value: DbValue[A])                                            extends SqlDbValue[Option[A]]
    case BinOp[A, B, R](lhs: DbValue[A], rhs: DbValue[B], op: platform.BinOp[A, B, R]) extends SqlDbValue[R]
    case Function(name: SqlExpr.FunctionName, values: Seq[AnyDbValue])

    override def ast: SqlExpr = this match
      case SqlDbValue.DbColumn(_)                      => throw new IllegalArgumentException("Value not tagged")
      case SqlDbValue.QueryColumn(queryName, fromName) => SqlExpr.QueryRef(fromName, queryName)
      case SqlDbValue.GroupBy(value)                   => value.ast
      case SqlDbValue.BinOp(lhs, rhs, op)              => SqlExpr.BinOp(lhs.ast, rhs.ast, op.ast)
      case SqlDbValue.JoinNullable(value)              => value.ast
      case SqlDbValue.Function(f, values)              => SqlExpr.FunctionCall(f, values.map(_.ast))
    end ast

    override def hasGroupBy: Boolean = this match
      case SqlDbValue.DbColumn(_)         => false
      case SqlDbValue.QueryColumn(_, _)   => false
      case SqlDbValue.GroupBy(_)          => true
      case SqlDbValue.BinOp(lhs, rhs, _)  => lhs.hasGroupBy || rhs.hasGroupBy
      case SqlDbValue.JoinNullable(value) => value.hasGroupBy
      case SqlDbValue.Function(_, values) => values.exists(_.hasGroupBy)
    end hasGroupBy

    override def asSqlDbVal: Option[SqlDbValue[A]] = Some(this)
  }

  extension [A](sqlDbValue: SqlDbValue[A]) def liftSqlDbValue: DbValue[A]

  extension [A](dbVal: DbValue[A])

    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Eq().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Neq().liftSqlBinOp).liftSqlDbValue

    @targetName("dbValAsGrouped") protected[platform] inline def asGrouped: Grouped[A] = dbVal

    @targetName("dbValAsMany") protected[platform] inline def dbValAsMany: Many[A] = dbVal

    @targetName("dbValAsAnyDbVal") protected[platform] def asAnyDbVal: AnyDbValue

  extension (boolVal: DbValue[Boolean])
    @targetName("dbValBooleanAnd") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.And.liftSqlBinOp).liftSqlDbValue

    @targetName("dbValBooleanOr") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.Or.liftSqlBinOp).liftSqlDbValue

  trait SqlOrdSeqBase {
    def ast: Seq[SelectAst.OrderExpr]
  }

  type OrdSeq <: SqlOrdSeqBase

  opaque type Grouped[A] = DbValue[A]
  opaque type Many[A]    = DbValue[A]

  extension [A](grouped: Grouped[A])
    @targetName("groupedAsMany")
    inline def asMany: Many[A] = grouped

    @targetName("groupedAsDbValue") protected[platform] inline def asDbValue: DbValue[A] = grouped

  extension [A](many: Many[A]) @targetName("manyAsDbValue") protected[platform] inline def asDbValue: DbValue[A] = many
}
