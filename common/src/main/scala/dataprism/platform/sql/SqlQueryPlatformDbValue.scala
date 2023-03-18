package dataprism.platform.sql

import scala.annotation.targetName

import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.{Column, DbType, SqlArg}
import perspective.*

trait SqlQueryPlatformDbValue { platform: SqlQueryPlatform =>

  trait SqlBinOpBase {
    def ast: SqlExpr.BinaryOperation
  }

  type BinOp[LHS, RHS, R] <: SqlBinOpBase

  enum SqlBinOp[LHS, RHS, R](op: SqlExpr.BinaryOperation) extends SqlBinOpBase {
    case Eq[A]()             extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Eq)
    case Neq[A]()            extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Neq)
    case And                 extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolAnd)
    case Or                  extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolOr)
    case LessThan[A]()       extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.LessThan)
    case LessOrEqual[A]()    extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.LessOrEq)
    case GreaterThan[A]()    extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.GreaterThan)
    case GreaterOrEqual[A]() extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.GreaterOrEq)

    override def ast: SqlExpr.BinaryOperation = op
  }

  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait SqlDbValueBase[A] {

    def ast: SqlExpr

    def asSqlDbVal: Option[SqlDbValue[A]]
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  enum SqlDbValue[A] extends SqlDbValueBase[A] {
    case DbColumn(column: Column[A])
    case QueryColumn(queryName: String, fromName: String)
    case JoinNullable[B](value: DbValue[B])                                            extends SqlDbValue[Nullable[B]]
    case BinOp[B, C, R](lhs: DbValue[B], rhs: DbValue[C], op: platform.BinOp[B, C, R]) extends SqlDbValue[R]
    case Function(name: SqlExpr.FunctionName, values: Seq[AnyDbValue])
    case Placeholder(value: A, tpe: DbType[A])

    override def ast: SqlExpr = this match
      case SqlDbValue.DbColumn(_)                      => throw new IllegalArgumentException("Value not tagged")
      case SqlDbValue.QueryColumn(queryName, fromName) => SqlExpr.QueryRef(fromName, queryName)
      case SqlDbValue.BinOp(lhs, rhs, op)              => SqlExpr.BinOp(lhs.ast, rhs.ast, op.ast)
      case SqlDbValue.JoinNullable(value)              => value.ast
      case SqlDbValue.Function(f, values)              => SqlExpr.FunctionCall(f, values.map(_.ast))
      case SqlDbValue.Placeholder(value, tpe)          => SqlExpr.PreparedArgument(None, SqlArg.SqlArgObj(value, tpe))
    end ast

    override def asSqlDbVal: Option[SqlDbValue[A]] = Some(this)
  }

  extension [A](sqlDbValue: SqlDbValue[A]) def liftSqlDbValue: DbValue[A]

  extension [A](v: A) def as(tpe: DbType[A]): DbValue[A] = SqlDbValue.Placeholder(v, tpe).liftSqlDbValue

  extension [A](dbVal: DbValue[A])

    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Eq().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Neq().liftSqlBinOp).liftSqlDbValue

    @targetName("dbValAsMany") protected[platform] inline def dbValAsMany: Many[A] = dbVal

    @targetName("dbValAsAnyDbVal") protected[platform] def asAnyDbVal: AnyDbValue

  extension (boolVal: DbValue[Boolean])
    @targetName("dbValBooleanAnd") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.And.liftSqlBinOp).liftSqlDbValue

    @targetName("dbValBooleanOr") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.Or.liftSqlBinOp).liftSqlDbValue

  extension (intVal: DbValue[Int])
    @targetName("dbValIntLessThan") def <(that: DbValue[Int]): DbValue[Boolean] =
      SqlDbValue.BinOp(intVal, that, SqlBinOp.LessThan().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValIntLessOrEqual") def <=(that: DbValue[Int]): DbValue[Boolean] =
      SqlDbValue.BinOp(intVal, that, SqlBinOp.LessOrEqual().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValIntGreaterThan") def >(that: DbValue[Int]): DbValue[Boolean] =
      SqlDbValue.BinOp(intVal, that, SqlBinOp.GreaterThan().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValIntGreaterOrEqual") def >=(that: DbValue[Int]): DbValue[Boolean] =
      SqlDbValue.BinOp(intVal, that, SqlBinOp.GreaterOrEqual().liftSqlBinOp).liftSqlDbValue

  trait SqlOrdSeqBase {
    def ast: Seq[SelectAst.OrderExpr]
  }

  type OrdSeq <: SqlOrdSeqBase

  opaque type Many[A] = DbValue[A]

  extension [A](many: Many[A]) @targetName("manyAsDbValue") protected[platform] inline def asDbValue: DbValue[A] = many
}
