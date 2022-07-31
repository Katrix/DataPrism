package dataprism.platform

import scala.annotation.targetName

import cats.syntax.all.*
import dataprism.sql.*

trait SqlQueryPlatform extends QueryPlatform { platform =>

  type BinOp[LHS, RHS, R]
  enum SqlBinOp[LHS, RHS, R] {
    case Eq[A]()  extends SqlBinOp[A, A, Boolean]
    case Neq[A]() extends SqlBinOp[A, A, Boolean]
    case And      extends SqlBinOp[Boolean, Boolean, Boolean]
    case Or       extends SqlBinOp[Boolean, Boolean, Boolean]
  }
  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  type AnyDbValue <: DbValue[Any]

  enum SqlDbValue[A] {
    case DbColumn(column: Column[A])
    case QueryColumn(queryName: String, fromName: String)
    case GroupBy(value: DbValue[A])
    case JoinNullable[A](value: DbValue[A])                                            extends SqlDbValue[Option[A]]
    case BinOp[A, B, R](lhs: DbValue[A], rhs: DbValue[B], op: platform.BinOp[A, B, R]) extends SqlDbValue[R]
    case Function(name: String, values: Seq[AnyDbValue])
    case AddAlias(value: DbValue[A], alias: String)

    def render: SqlStr = this match
      case SqlDbValue.DbColumn(_)                      => throw new IllegalArgumentException("Value not tagged")
      case SqlDbValue.QueryColumn(queryName, fromName) => sql"${SqlStr.const(queryName)}.${SqlStr.const(fromName)}"
      case SqlDbValue.GroupBy(value)                   => value.render
      case SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Eq())   => sql"${lhs.render} = ${rhs.render}"
      case SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Neq())  => sql"${lhs.render} != ${rhs.render}"
      case SqlDbValue.BinOp(lhs, rhs, SqlBinOp.And)    => sql"${lhs.render} AND ${rhs.render}"
      case SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Or)     => sql"${lhs.render} OR ${rhs.render}"
      case SqlDbValue.JoinNullable(value)              => value.render
      case SqlDbValue.Function(f, values) =>
        sql"${SqlStr.const(f)}(${values.map(_.render).intercalate(sql", ")})"
      case SqlDbValue.AddAlias(value, alias) => sql"${value.render} AS ${SqlStr.const(alias)}"
    end render

    def hasGroupBy: Boolean = this match
      case SqlDbValue.DbColumn(_)         => false
      case SqlDbValue.QueryColumn(_, _)   => false
      case SqlDbValue.GroupBy(_)          => true
      case SqlDbValue.BinOp(lhs, rhs, _)  => lhs.hasGroupBy || rhs.hasGroupBy
      case SqlDbValue.JoinNullable(value) => value.hasGroupBy
      case SqlDbValue.Function(_, values) => values.exists(_.hasGroupBy)
      case SqlDbValue.AddAlias(value, _)  => value.hasGroupBy
    end hasGroupBy
  }
  extension [A](sqlDbValue: SqlDbValue[A]) def liftSqlDbValue: DbValue[A]

  extension [A](dbVal: DbValue[A])
    @targetName("dbValueRender") def render: SqlStr
    @targetName("dbValueRender") def hasGroupBy: Boolean

    @targetName("dbValueAsGrouped") protected inline def asGrouped: Grouped[A] = dbVal
    @targetName("dbValueAsMany") protected inline def dbValAsMany: Many[A]     = dbVal
    @targetName("dbValueAsAnyDbValue") protected inline def asAnyDbVal: AnyDbValue

    @targetName("dbValEquals") def ===(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Eq().liftSqlBinOp).liftSqlDbValue
    @targetName("dbValNotEquals") def !==(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(dbVal, that, SqlBinOp.Neq().liftSqlBinOp).liftSqlDbValue

  extension (boolVal: DbValue[Boolean])
    @targetName("dbValBooleanAnd") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.And.liftSqlBinOp).liftSqlDbValue
    @targetName("dbValBooleanOr") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.Or.liftSqlBinOp).liftSqlDbValue

  extension (ordSeq: OrdSeq) @targetName("ordSeqRender") def render: SqlStr

  opaque type Grouped[A] = DbValue[A]
  opaque type Many[A]    = DbValue[A]

  extension [A](grouped: Grouped[A])
    @targetName("groupedAsMany")
    inline def asMany: Many[A] = grouped

    @targetName("groupedAsDbValue") protected inline def asDbValue: DbValue[A] = grouped

  extension [A](many: Many[A]) @targetName("manyAsDbValue") protected inline def asDbValue: DbValue[A] = many
}
