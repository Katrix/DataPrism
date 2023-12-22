package dataprism.platform.sql

import scala.annotation.targetName

import cats.data.State
import cats.syntax.all.*
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlQueryPlatformDbValue { platform: SqlQueryPlatform =>

  trait SqlUnaryOpBase[R] {
    def ast: SqlExpr.UnaryOperation

    def tpe: Type[R]
  }

  type UnaryOp[V, R] <: SqlUnaryOpBase[R]

  trait SqlBinOpBase[R] {
    def ast: SqlExpr.BinaryOperation

    def tpe: Type[R]
  }

  extension [A](tpe: Type[A]) @targetName("typeName") def name: String

  type BinOp[LHS, RHS, R] <: SqlBinOpBase[R]

  enum SqlUnaryOp[V, R](op: SqlExpr.UnaryOperation) extends SqlUnaryOpBase[R] {
    case Not extends SqlUnaryOp[Boolean, Boolean](SqlExpr.UnaryOperation.Not)

    override def ast: SqlExpr.UnaryOperation = op

    override def tpe: Type[R] = this match
      case Not => AnsiTypes.boolean
  }

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R]

  enum SqlBinOp[LHS, RHS, R](op: SqlExpr.BinaryOperation) extends SqlBinOpBase[R] {
    case Eq[A]()             extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Eq)
    case Neq[A]()            extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.Neq)
    case And                 extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolAnd)
    case Or                  extends SqlBinOp[Boolean, Boolean, Boolean](SqlExpr.BinaryOperation.BoolOr)
    case LessThan[A]()       extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.LessThan)
    case LessOrEqual[A]()    extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.LessOrEq)
    case GreaterThan[A]()    extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.GreaterThan)
    case GreaterOrEqual[A]() extends SqlBinOp[A, A, Boolean](SqlExpr.BinaryOperation.GreaterOrEq)

    case Plus[A](numeric: SqlNumeric[A])     extends SqlBinOp[A, A, A](SqlExpr.BinaryOperation.Plus)
    case Minus[A](numeric: SqlNumeric[A])    extends SqlBinOp[A, A, A](SqlExpr.BinaryOperation.Minus)
    case Multiply[A](numeric: SqlNumeric[A]) extends SqlBinOp[A, A, A](SqlExpr.BinaryOperation.Multiply)
    case Divide[A](numeric: SqlNumeric[A])   extends SqlBinOp[A, A, A](SqlExpr.BinaryOperation.Divide)

    override def ast: SqlExpr.BinaryOperation = op

    override def tpe: Type[R] = this match
      case Eq()             => AnsiTypes.boolean
      case Neq()            => AnsiTypes.boolean
      case And              => AnsiTypes.boolean
      case Or               => AnsiTypes.boolean
      case LessThan()       => AnsiTypes.boolean
      case LessOrEqual()    => AnsiTypes.boolean
      case GreaterThan()    => AnsiTypes.boolean
      case GreaterOrEqual() => AnsiTypes.boolean

      case Plus(numeric)     => numeric.tpe
      case Minus(numeric)    => numeric.tpe
      case Multiply(numeric) => numeric.tpe
      case Divide(numeric)   => numeric.tpe
    end tpe
  }

  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait SqlNumeric[A]:
    def tpe: Type[A]

    extension (lhs: DbValue[A])
      def +(rhs: DbValue[A]): DbValue[A]
      def -(rhs: DbValue[A]): DbValue[A]
      def *(rhs: DbValue[A]): DbValue[A]
      def /(rhs: DbValue[A]): DbValue[A]

      // TODO: Having these in here is quite broad. Might want to tighten this
      def avg: DbValue[Nullable[A]] =
        SqlDbValue.Function(SqlExpr.FunctionName.Avg, Seq(lhs.unsafeAsAnyDbVal), AnsiTypes.nullable(lhs.tpe)).lift
      def sum: DbValue[Nullable[A]] =
        SqlDbValue.Function(SqlExpr.FunctionName.Sum, Seq(lhs.unsafeAsAnyDbVal), AnsiTypes.nullable(lhs.tpe)).lift

  object SqlNumeric:
    def defaultInstance[A](tpe0: Type[A]): SqlNumeric[A] = new SqlNumeric[A]:
      override def tpe: Type[A] = tpe0

      extension (lhs: DbValue[A])
        override def +(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Plus(this).liftSqlBinOp).lift
        override def -(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Minus(this).liftSqlBinOp).lift
        override def *(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Multiply(this).liftSqlBinOp).lift
        override def /(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Divide(this).liftSqlBinOp).lift

  given SqlNumeric[Int]    = SqlNumeric.defaultInstance(AnsiTypes.integer)
  given SqlNumeric[Long]   = SqlNumeric.defaultInstance(AnsiTypes.bigint)
  given SqlNumeric[Float]  = SqlNumeric.defaultInstance(AnsiTypes.real)
  given SqlNumeric[Double] = SqlNumeric.defaultInstance(AnsiTypes.doublePrecision)

  trait SqlOrdered[A]:
    def tpe: Type[A]

    extension (lhs: DbValue[A])
      def <(rhs: DbValue[A]): DbValue[Boolean]
      def <=(rhs: DbValue[A]): DbValue[Boolean]
      def >=(rhs: DbValue[A]): DbValue[Boolean]
      def >(rhs: DbValue[A]): DbValue[Boolean]

      // TODO: Having these in here is quite broad. Might want to tighten this
      def min: DbValue[Nullable[A]] =
        SqlDbValue.Function(SqlExpr.FunctionName.Least, Seq(lhs.unsafeAsAnyDbVal), AnsiTypes.nullable(lhs.tpe)).lift
      def max: DbValue[Nullable[A]] =
        SqlDbValue.Function(SqlExpr.FunctionName.Greatest, Seq(lhs.unsafeAsAnyDbVal), AnsiTypes.nullable(lhs.tpe)).lift

  object SqlOrdered:
    def defaultInstance[A](tpe0: Type[A]): SqlOrdered[A] = new SqlOrdered[A]:
      override def tpe: Type[A] = tpe0

      extension (lhs: DbValue[A])
        def <(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessThan().liftSqlBinOp).lift
        def <=(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessOrEqual().liftSqlBinOp).lift
        def >=(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterOrEqual().liftSqlBinOp).lift
        def >(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterThan().liftSqlBinOp).lift

  given SqlOrdered[Int]    = SqlOrdered.defaultInstance(AnsiTypes.integer)
  given SqlOrdered[Long]   = SqlOrdered.defaultInstance(AnsiTypes.bigint)
  given SqlOrdered[Float]  = SqlOrdered.defaultInstance(AnsiTypes.real)
  given SqlOrdered[Double] = SqlOrdered.defaultInstance(AnsiTypes.doublePrecision)

  trait SqlDbValueBase[A] extends DbValueBase[A] {

    @targetName("dbEquals") override def ===(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(this.liftDbValue, that, SqlBinOp.Eq().liftSqlBinOp).lift

    @targetName("dbNotEquals") override def !==(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(this.liftDbValue, that, SqlBinOp.Neq().liftSqlBinOp).lift

    def cast[B](tpe: Type[B]): DbValue[B] = SqlDbValue.Cast(this.unsafeAsAnyDbVal, tpe).lift

    def ast: TagState[SqlExpr[Type]]

    def asSqlDbVal: Option[SqlDbValue[A]]

    def tpe: Type[A]

    def unsafeAsAnyDbVal: AnyDbValue
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  enum SqlDbValue[A] extends SqlDbValueBase[A] {
    case DbColumn(column: Column[A, Type])
    case QueryColumn(queryName: String, fromName: String, override val tpe: Type[A])
    case JoinNullable[B](value: DbValue[B])                                            extends SqlDbValue[Nullable[B]]
    case UnaryOp[B, R](value: DbValue[B], op: platform.UnaryOp[B, R])                  extends SqlDbValue[R]
    case BinOp[B, C, R](lhs: DbValue[B], rhs: DbValue[C], op: platform.BinOp[B, C, R]) extends SqlDbValue[R]
    case Function(name: SqlExpr.FunctionName, values: Seq[AnyDbValue], override val tpe: Type[A])
    case Cast(value: AnyDbValue, override val tpe: Type[A])
    case Placeholder(value: A, override val tpe: Type[A])
    case CompilePlaceholder(identifier: Object, override val tpe: Type[A])
    case SubSelect(query: Query[[F[_]] =>> F[A]])
    case QueryCount extends SqlDbValue[Long]

    override def ast: TagState[SqlExpr[Type]] = this match
      case SqlDbValue.DbColumn(_)                         => throw new IllegalArgumentException("Value not tagged")
      case SqlDbValue.QueryColumn(queryName, fromName, _) => State.pure(SqlExpr.QueryRef(fromName, queryName))
      case SqlDbValue.UnaryOp(value, op)                  => value.ast.map(v => SqlExpr.UnaryOp(v, op.ast))
      case SqlDbValue.BinOp(lhs, rhs, op) =>
        lhs.ast.flatMap(l => rhs.ast.map(r => SqlExpr.BinOp(l, r, op.ast)))
      case SqlDbValue.JoinNullable(value) => value.ast
      case SqlDbValue.Function(f, values, _) =>
        values.toList.traverse(_.ast).map(exprs => SqlExpr.FunctionCall(f, exprs))
      case SqlDbValue.Cast(value, tpe) => value.ast.map(v => SqlExpr.Cast(v, tpe.name))
      case SqlDbValue.Placeholder(value, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.SqlArgObj(value, tpe)))
      case SqlDbValue.CompilePlaceholder(identifier, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.CompileArg(identifier, tpe)))
      case SqlDbValue.SubSelect(query) => query.selectAstAndValues.map(m => SqlExpr.SubSelect(m.ast))
      case SqlDbValue.QueryCount       => State.pure(SqlExpr.QueryCount())
    end ast

    override def asSqlDbVal: Option[SqlDbValue[A]] = Some(this)

    override def tpe: Type[A] = this match
      case SqlDbValue.DbColumn(col)              => col.tpe
      case SqlDbValue.QueryColumn(_, _, tpe)     => tpe
      case SqlDbValue.UnaryOp(_, op)             => op.tpe
      case SqlDbValue.BinOp(_, _, op)            => op.tpe
      case SqlDbValue.JoinNullable(value)        => AnsiTypes.nullable(value.tpe).asInstanceOf[Type[A]]
      case SqlDbValue.Function(_, _, tpe)        => tpe
      case SqlDbValue.Cast(_, tpe)               => tpe
      case SqlDbValue.Placeholder(_, tpe)        => tpe
      case SqlDbValue.CompilePlaceholder(_, tpe) => tpe
      case SqlDbValue.SubSelect(query)           => query.selectAstAndValues.runA(freshTaggedState).value.values.tpe
      case SqlDbValue.QueryCount                 => AnsiTypes.bigint
    end tpe

    override def unsafeAsAnyDbVal: AnyDbValue = this.lift.unsafeAsAnyDbVal

    override def liftDbValue: DbValue[A] = this.lift

    override def asc: Ord = this.lift.asc

    override def desc: Ord = this.lift.asc
  }
  given [A]: Lift[SqlDbValue[A], DbValue[A]] = sqlDbValueLift[A]

  protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]]

  extension [A](dbValue: DbValue[A])
    @targetName("dbValueAsMany") protected inline def unsafeDbValAsMany: Many[A] = dbValue.asInstanceOf[Many[A]]

  extension [A](v: A) def as(tpe: Type[A]): DbValue[A] = SqlDbValue.Placeholder(v, tpe).lift

  extension (boolVal: DbValue[Boolean])
    @targetName("dbValBooleanAnd") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.And.liftSqlBinOp).lift

    @targetName("dbValBooleanOr") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.Or.liftSqlBinOp).lift

    @targetName("dbValBooleanNot") def unary_! : DbValue[Boolean] =
      SqlDbValue.UnaryOp(boolVal, SqlUnaryOp.Not.liftSqlUnaryOp).lift

  trait SqlOrdSeqBase extends OrdSeqBase {
    def ast: TagState[Seq[SelectAst.OrderExpr[Type]]]
  }

  type OrdSeq <: SqlOrdSeqBase

  opaque type Many[A] = DbValue[Any] // Scala compiler bug? Stack overflow
  object Many {
    extension [A](many: Many[A])
      // TODO: Check that the return type is indeed Long on all platforms
      def count: DbValue[Long] =
        SqlDbValue.Function(SqlExpr.FunctionName.Count, Seq(many.unsafeAsDbValue.unsafeAsAnyDbVal), AnsiTypes.bigint).lift

      inline def unsafeAsDbValue: DbValue[A] = many.asInstanceOf[DbValue[A]]
  }
}
