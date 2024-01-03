package dataprism.platform.sql

import java.sql.{Date, Time, Timestamp}

import scala.annotation.targetName
import scala.util.NotGiven

import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlQueryPlatformDbValue { platform: SqlQueryPlatform =>

  trait SqlUnaryOpBase[V, R] {
    def ast: SqlExpr.UnaryOperation

    def tpe(v: DbValue[V]): Type[R]
  }

  type UnaryOp[V, R] <: SqlUnaryOpBase[V, R]

  trait SqlBinOpBase[LHS, RHS, R] {
    def ast: SqlExpr.BinaryOperation

    def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[R]
  }

  extension [A](tpe: Type[A]) @targetName("typeName") def name: String

  type BinOp[LHS, RHS, R] <: SqlBinOpBase[LHS, RHS, R]

  enum SqlUnaryOp[V, R](op: SqlExpr.UnaryOperation) extends SqlUnaryOpBase[V, R] {
    case Not                                 extends SqlUnaryOp[Boolean, Boolean](SqlExpr.UnaryOperation.Not)
    case Negative[A](numeric: SqlNumeric[A]) extends SqlUnaryOp[A, A](SqlExpr.UnaryOperation.Negation)
    case NullableOp[V1, R1](op: UnaryOp[V1, R1], ev: NotGiven[R1 <:< Option[_]])
        extends SqlUnaryOp[Nullable[V1], Nullable[R1]](op.ast)

    override def ast: SqlExpr.UnaryOperation = op

    override def tpe(v: DbValue[V]): Type[R] = this match
      case Not               => AnsiTypes.boolean
      case Negative(numeric) => numeric.tpe(v, v)
      case nop: NullableOp[v1, _] => AnsiTypes.nullable(nop.op.tpe(v.asInstanceOf[DbValue[v1]]))(using nop.ev)
  }

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R]

  enum SqlBinOp[LHS, RHS, R](op: SqlExpr.BinaryOperation) extends SqlBinOpBase[LHS, RHS, R] {
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

    case NullableOp[LHS1, RHS1, R1](binop: BinOp[LHS1, RHS1, R1], ev: NotGiven[R1 <:< Option[_]])
        extends SqlBinOp[Nullable[LHS1], Nullable[RHS1], Nullable[R1]](binop.ast)

    override def ast: SqlExpr.BinaryOperation = op

    override def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[R] = this match
      case Eq()             => AnsiTypes.boolean
      case Neq()            => AnsiTypes.boolean
      case And              => AnsiTypes.boolean
      case Or               => AnsiTypes.boolean
      case LessThan()       => AnsiTypes.boolean
      case LessOrEqual()    => AnsiTypes.boolean
      case GreaterThan()    => AnsiTypes.boolean
      case GreaterOrEqual() => AnsiTypes.boolean

      case Plus(numeric)     => numeric.tpe(lhs, rhs)
      case Minus(numeric)    => numeric.tpe(lhs, rhs)
      case Multiply(numeric) => numeric.tpe(lhs, rhs)
      case Divide(numeric)   => numeric.tpe(lhs, rhs)

      case nop: NullableOp[lhs1, rhs1, _] =>
        AnsiTypes.nullable(nop.binop.tpe(lhs.asInstanceOf[DbValue[lhs1]], rhs.asInstanceOf[DbValue[rhs1]]))(
          using nop.ev
        )
    end tpe
  }

  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait SqlNumeric[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A]

    extension (lhs: DbValue[A])
      @targetName("plus") def +(rhs: DbValue[A]): DbValue[A]
      @targetName("minus") def -(rhs: DbValue[A]): DbValue[A]
      @targetName("times") def *(rhs: DbValue[A]): DbValue[A]
      @targetName("divide") def /(rhs: DbValue[A]): DbValue[A]
      @targetName("negation") def unary_- : DbValue[A]

    extension (lhs: Many[A])
      // TODO: Having these in here is quite broad. Might want to tighten this
      def avg(using ev: NotGiven[A <:< Option[_]]): DbValue[Nullable[A]] =
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Avg,
            Seq(lhs.unsafeAsAnyDbVal),
            AnsiTypes.nullable(lhs.asInstanceOf[DbValue[A]].tpe)
          )
          .lift
      def sum(using ev: NotGiven[A <:< Option[_]]): DbValue[Nullable[A]] =
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Sum,
            Seq(lhs.unsafeAsAnyDbVal),
            AnsiTypes.nullable(lhs.asInstanceOf[DbValue[A]].tpe)
          )
          .lift

    extension (lhs: DbValue[Nullable[A]])
      @targetName("plusNullable") def +(rhs: DbValue[Nullable[A]])(
          using ev: NotGiven[A <:< Option[_]]
      ): DbValue[Nullable[A]]
      @targetName("minusNullable") def -(rhs: DbValue[Nullable[A]])(
          using ev: NotGiven[A <:< Option[_]]
      ): DbValue[Nullable[A]]
      @targetName("timesNullable") def *(rhs: DbValue[Nullable[A]])(
          using ev: NotGiven[A <:< Option[_]]
      ): DbValue[Nullable[A]]
      @targetName("divideNullable") def /(rhs: DbValue[Nullable[A]])(
          using ev: NotGiven[A <:< Option[_]]
      ): DbValue[Nullable[A]]
      @targetName("negationNullable") def unary_-(
          using ev: NotGiven[A <:< Option[_]]
      ): DbValue[Nullable[A]]

    extension (lhs: Many[Nullable[A]])
      // TODO: Having these in here is quite broad. Might want to tighten this
      @targetName("avgNullable") def avg: DbValue[Nullable[A]] =
        SqlDbValue
          .Function(SqlExpr.FunctionName.Avg, Seq(lhs.unsafeAsAnyDbVal), lhs.asInstanceOf[DbValue[Nullable[A]]].tpe)
          .lift
      @targetName("sumNullable") def sum: DbValue[Nullable[A]] =
        SqlDbValue
          .Function(SqlExpr.FunctionName.Sum, Seq(lhs.unsafeAsAnyDbVal), lhs.asInstanceOf[DbValue[Nullable[A]]].tpe)
          .lift

  object SqlNumeric:
    def defaultInstance[A]: SqlNumeric[A] = new SqlNumeric[A]:
      override def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = rhs.tpe

      extension (lhs: DbValue[A])
        @targetName("plus") override def +(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Plus(this).liftSqlBinOp).lift
        @targetName("minus") override def -(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Minus(this).liftSqlBinOp).lift
        @targetName("times") override def *(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Multiply(this).liftSqlBinOp).lift
        @targetName("divide") override def /(rhs: DbValue[A]): DbValue[A] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Divide(this).liftSqlBinOp).lift
        @targetName("negation") def unary_- : DbValue[A] =
          SqlDbValue.UnaryOp(lhs, SqlUnaryOp.Negative(this).liftSqlUnaryOp).lift

      extension (lhs: DbValue[Nullable[A]])
        @targetName("plusNullable") def +(rhs: DbValue[Nullable[A]])(
            using ev: NotGiven[A <:< Option[_]]
        ): DbValue[Nullable[A]] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.NullableOp(SqlBinOp.Plus(this).liftSqlBinOp, ev).liftSqlBinOp).lift
        @targetName("minusNullable") def -(rhs: DbValue[Nullable[A]])(
            using ev: NotGiven[A <:< Option[_]]
        ): DbValue[Nullable[A]] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.NullableOp(SqlBinOp.Minus(this).liftSqlBinOp, ev).liftSqlBinOp).lift
        @targetName("timesNullable") def *(rhs: DbValue[Nullable[A]])(
            using ev: NotGiven[A <:< Option[_]]
        ): DbValue[Nullable[A]] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.NullableOp(SqlBinOp.Multiply(this).liftSqlBinOp, ev).liftSqlBinOp).lift
        @targetName("divideNullable") def /(rhs: DbValue[Nullable[A]])(
            using ev: NotGiven[A <:< Option[_]]
        ): DbValue[Nullable[A]] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.NullableOp(SqlBinOp.Divide(this).liftSqlBinOp, ev).liftSqlBinOp).lift

        @targetName("negationNullable") def unary_-(
            using ev: NotGiven[A <:< Option[_]]
        ): DbValue[Nullable[A]] =
          SqlDbValue.UnaryOp(lhs, SqlUnaryOp.NullableOp(SqlUnaryOp.Negative(this).liftSqlUnaryOp, ev).liftSqlUnaryOp).lift

  given SqlNumeric[Short]      = SqlNumeric.defaultInstance
  given SqlNumeric[Int]        = SqlNumeric.defaultInstance
  given SqlNumeric[Long]       = SqlNumeric.defaultInstance
  given SqlNumeric[Float]      = SqlNumeric.defaultInstance
  given SqlNumeric[Double]     = SqlNumeric.defaultInstance
  given SqlNumeric[BigDecimal] = SqlNumeric.defaultInstance

  trait SqlOrdered[A]:
    def greatest(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Greatest, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    def least(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Least, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    @targetName("greatestNullable") def greatest(
        head: DbValue[Nullable[A]],
        tail: DbValue[Nullable[A]]*
    ): DbValue[Nullable[A]] =
      SqlDbValue.Function(SqlExpr.FunctionName.Greatest, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    @targetName("leastNullable") def least(
        head: DbValue[Nullable[A]],
        tail: DbValue[Nullable[A]]*
    ): DbValue[Nullable[A]] =
      SqlDbValue.Function(SqlExpr.FunctionName.Least, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    extension (lhs: DbValue[A])
      @targetName("lessThan") def <(rhs: DbValue[A]): DbValue[Boolean]
      @targetName("lessOrEqual") def <=(rhs: DbValue[A]): DbValue[Boolean]
      @targetName("greaterOrEqual") def >=(rhs: DbValue[A]): DbValue[Boolean]
      @targetName("greatherThan") def >(rhs: DbValue[A]): DbValue[Boolean]

      @targetName("leastExtension") def least(rhss: DbValue[A]*): DbValue[A]       = this.least(lhs, rhss*)
      @targetName("greatestExtension") def greatest(rhss: DbValue[A]*): DbValue[A] = this.greatest(lhs, rhss*)

    extension (lhs: DbValue[Nullable[A]])
      @targetName("lessThanNullable") def <(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]]
      @targetName("lessOrEqualNullable") def <=(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]]
      @targetName("greaterOrEqualNullable") def >=(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]]
      @targetName("greatherThanNullable") def >(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]]

      @targetName("leastNullableExtension") def least(rhss: DbValue[Nullable[A]]*): DbValue[Nullable[A]] =
        this.least(lhs, rhss*)
      @targetName("greatestNullableExtension") def greatest(rhss: DbValue[Nullable[A]]*): DbValue[Nullable[A]] =
        this.greatest(lhs, rhss*)

    extension (lhs: Many[A])
      // TODO: Having these in here is quite broad. Might want to tighten this
      def min(using ev: NotGiven[A <:< Option[_]]): DbValue[Nullable[A]] =
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Min,
            Seq(lhs.unsafeAsAnyDbVal),
            AnsiTypes.nullable(lhs.asInstanceOf[DbValue[A]].tpe)
          )
          .lift
      def max(using ev: NotGiven[A <:< Option[_]]): DbValue[Nullable[A]] =
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Max,
            Seq(lhs.unsafeAsAnyDbVal),
            AnsiTypes.nullable(lhs.asInstanceOf[DbValue[A]].tpe)
          )
          .lift

    extension (lhs: Many[Nullable[A]])
      // TODO: Having these in here is quite broad. Might want to tighten this
      @targetName("minNullable") def min: DbValue[Nullable[A]] =
        SqlDbValue
          .Function(SqlExpr.FunctionName.Min, Seq(lhs.unsafeAsAnyDbVal), lhs.asInstanceOf[DbValue[Nullable[A]]].tpe)
          .lift
      @targetName("maxNullable") def max: DbValue[Nullable[A]] =
        SqlDbValue
          .Function(SqlExpr.FunctionName.Max, Seq(lhs.unsafeAsAnyDbVal), lhs.asInstanceOf[DbValue[Nullable[A]]].tpe)
          .lift

  object SqlOrdered:
    def defaultInstance[A]: SqlOrdered[A] = new SqlOrdered[A]:
      extension (lhs: DbValue[A])
        @targetName("lessThan") def <(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessThan().liftSqlBinOp).lift
        @targetName("lessOrEqual") def <=(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessOrEqual().liftSqlBinOp).lift
        @targetName("greaterOrEqual") def >=(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterOrEqual().liftSqlBinOp).lift
        @targetName("greatherThan") def >(rhs: DbValue[A]): DbValue[Boolean] =
          SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterThan().liftSqlBinOp).lift

      extension (lhs: DbValue[Nullable[A]])
        @targetName("lessThanNullable") def <(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]] =
          SqlDbValue
            .BinOp(
              lhs,
              rhs,
              SqlBinOp
                .NullableOp(SqlBinOp.LessThan().liftSqlBinOp, summon[NotGiven[Boolean <:< Option[_]]])
                .liftSqlBinOp
            )
            .lift
        @targetName("lessOrEqualNullable") def <=(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]] =
          SqlDbValue
            .BinOp(
              lhs,
              rhs,
              SqlBinOp
                .NullableOp(SqlBinOp.LessOrEqual().liftSqlBinOp, summon[NotGiven[Boolean <:< Option[_]]])
                .liftSqlBinOp
            )
            .lift
        @targetName("greaterOrEqualNullable") def >=(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]] =
          SqlDbValue
            .BinOp(
              lhs,
              rhs,
              SqlBinOp
                .NullableOp(SqlBinOp.GreaterOrEqual().liftSqlBinOp, summon[NotGiven[Boolean <:< Option[_]]])
                .liftSqlBinOp
            )
            .lift
        @targetName("greatherThanNullable") def >(rhs: DbValue[Nullable[A]]): DbValue[Nullable[Boolean]] =
          SqlDbValue
            .BinOp(
              lhs,
              rhs,
              SqlBinOp
                .NullableOp(SqlBinOp.GreaterThan().liftSqlBinOp, summon[NotGiven[Boolean <:< Option[_]]])
                .liftSqlBinOp
            )
            .lift

  given SqlOrdered[Short]      = SqlOrdered.defaultInstance
  given SqlOrdered[Int]        = SqlOrdered.defaultInstance
  given SqlOrdered[Long]       = SqlOrdered.defaultInstance
  given SqlOrdered[Float]      = SqlOrdered.defaultInstance
  given SqlOrdered[Double]     = SqlOrdered.defaultInstance
  given SqlOrdered[String]     = SqlOrdered.defaultInstance
  given SqlOrdered[Date]       = SqlOrdered.defaultInstance
  given SqlOrdered[Time]       = SqlOrdered.defaultInstance
  given SqlOrdered[Timestamp]  = SqlOrdered.defaultInstance
  given SqlOrdered[BigDecimal] = SqlOrdered.defaultInstance

  type CastType[A]
  extension [A](t: CastType[A])
    def castTypeName: String
    def castTypeType: Type[A]

  trait SqlDbValueBase[A] extends DbValueBase[A] {

    @targetName("dbEquals") override def ===(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(this.liftDbValue, that, SqlBinOp.Eq().liftSqlBinOp).lift

    @targetName("dbNotEquals") override def !==(that: DbValue[A]): DbValue[Boolean] =
      SqlDbValue.BinOp(this.liftDbValue, that, SqlBinOp.Neq().liftSqlBinOp).lift

    def cast[B](tpe: CastType[B]): DbValue[B] =
      SqlDbValue.Cast(this.unsafeAsAnyDbVal, tpe.castTypeName, tpe.castTypeType).lift

    def asSome(using ev: NotGiven[A <:< Option[_]]): DbValue[Nullable[A]] = SqlDbValue.AsSome(this.liftDbValue, ev).lift

    def ast: TagState[SqlExpr[Type]]

    def asSqlDbVal: Option[SqlDbValue[A]]

    def tpe: Type[A]

    def unsafeAsAnyDbVal: AnyDbValue
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  type DbValueCompanion <: SqlDbValueCompanion
  trait SqlDbValueCompanion {
    def rawK[A[_[_]]: TraverseKC, B](args: A[DbValue], tpe: Type[B])(
        render: A[Const[SqlStr[Type]]] => SqlStr[Type]
    ): DbValue[B] =
      val argsList: List[AnyDbValue] = args.foldMapK([X] => (v: DbValue[X]) => List(v.unsafeAsAnyDbVal))
      val indicesState: State[Int, A[Const[Int]]] =
        args.traverseK([X] => (_: DbValue[X]) => State((acc: Int) => (acc + 1, acc)))
      val indices: A[Const[Int]] = indicesState.runA(0).value
      SqlDbValue.Custom(argsList, strArgs => render(indices.mapConst([X] => (i: Int) => strArgs(i))), tpe).lift

    inline def raw[T, A](args: T, tpe: Type[A])(using mr: MapRes[DbValue, T])(
        render: mr.K[Const[SqlStr[Type]]] => SqlStr[Type]
    ): DbValue[A] =
      rawK(mr.toK(args), tpe)(render)(using mr.traverseKC)

    def functionK[A[_[_]]: TraverseKC, B](name: String, tpe: Type[B])(args: A[DbValue]): DbValue[B] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Custom(name),
          args.foldMapK([X] => (v: DbValue[X]) => List(v.unsafeAsAnyDbVal)),
          tpe
        )
        .lift

    inline def function[T, B](name: String, tpe: Type[B])(args: T)(using mr: MapRes[DbValue, T]): DbValue[B] =
      functionK(name, tpe)(mr.toK(args))(using mr.traverseKC)
  }

  enum SqlDbValue[A] extends SqlDbValueBase[A] {
    case DbColumn(column: Column[A, Type])
    case QueryColumn(queryName: String, fromName: String, override val tpe: Type[A])
    case JoinNullable[B](value: DbValue[B])                                            extends SqlDbValue[Nullable[B]]
    case UnaryOp[B, R](value: DbValue[B], op: platform.UnaryOp[B, R])                  extends SqlDbValue[R]
    case BinOp[B, C, R](lhs: DbValue[B], rhs: DbValue[C], op: platform.BinOp[B, C, R]) extends SqlDbValue[R]
    case Function(name: SqlExpr.FunctionName, values: Seq[AnyDbValue], override val tpe: Type[A])
    case Cast(value: AnyDbValue, typeName: String, override val tpe: Type[A])
    case AsSome[B](value: DbValue[B], ev: NotGiven[B <:< Option[_]]) extends SqlDbValue[Nullable[B]]
    case Placeholder(value: A, override val tpe: Type[A])
    case CompilePlaceholder(identifier: Object, override val tpe: Type[A])
    case SubSelect(query: Query[[F[_]] =>> F[A]])
    case IsNull[B](value: DbValue[Option[B]])    extends SqlDbValue[Boolean]
    case IsNotNull[B](value: DbValue[Option[B]]) extends SqlDbValue[Boolean]
    case ValueCase[V, R](matchOn: DbValue[V], cases: IndexedSeq[(DbValue[V], DbValue[R])], orElse: DbValue[R])
        extends SqlDbValue[R]
    case ConditionCase(cases: IndexedSeq[(DbValue[Boolean], DbValue[A])], orElse: DbValue[A])
    case Custom(args: Seq[AnyDbValue], render: Seq[SqlStr[Type]] => SqlStr[Type], override val tpe: Type[A])

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
      case SqlDbValue.Cast(value, typeName, _) => value.ast.map(v => SqlExpr.Cast(v, typeName))
      case SqlDbValue.AsSome(value, _)         => value.ast
      case SqlDbValue.Placeholder(value, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.SqlArgObj(value, tpe)))
      case SqlDbValue.CompilePlaceholder(identifier, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.CompileArg(identifier, tpe)))
      case SqlDbValue.SubSelect(query) => query.selectAstAndValues.map(m => SqlExpr.SubSelect(m.ast))
      case SqlDbValue.IsNull(value)    => value.ast.map(v => SqlExpr.IsNull(v))
      case SqlDbValue.IsNotNull(value) => value.ast.map(v => SqlExpr.IsNotNull(v))
      case SqlDbValue.ValueCase(matchOn, cases, orElse) =>
        (
          matchOn.ast,
          cases.toVector.traverse(c => c._1.ast.product(c._2.ast)),
          orElse.ast
        ).mapN((m, cs, o) => SqlExpr.ValueCase(m, cs, o))

      case SqlDbValue.ConditionCase(cases, orElse) =>
        cases.toVector
          .traverse(c => c._1.ast.product(c._2.ast))
          .map2(orElse.ast)((cs, o) => SqlExpr.ConditionCase(cs, o))
      case SqlDbValue.Custom(args, render, _) =>
        args.toVector.traverse(_.ast).map(exprs => SqlExpr.Custom(exprs, render))
      case SqlDbValue.QueryCount => State.pure(SqlExpr.QueryCount())
    end ast

    override def asSqlDbVal: Option[SqlDbValue[A]] = Some(this)

    override def tpe: Type[A] = this match
      case SqlDbValue.DbColumn(col)              => col.tpe
      case SqlDbValue.QueryColumn(_, _, tpe)     => tpe
      case SqlDbValue.UnaryOp(v, op)             => op.tpe(v)
      case SqlDbValue.BinOp(lhs, rhs, op)        => op.tpe(lhs, rhs)
      case SqlDbValue.JoinNullable(value)        => AnsiTypes.nullable(value.tpe).asInstanceOf[Type[A]]
      case SqlDbValue.Function(_, _, tpe)        => tpe
      case SqlDbValue.Cast(_, _, tpe)            => tpe
      case SqlDbValue.AsSome(value, ev)          => AnsiTypes.nullable(value.tpe)(using ev)
      case SqlDbValue.Placeholder(_, tpe)        => tpe
      case SqlDbValue.CompilePlaceholder(_, tpe) => tpe
      case SqlDbValue.SubSelect(query)           => query.selectAstAndValues.runA(freshTaggedState).value.values.tpe
      case SqlDbValue.IsNull(_)                  => AnsiTypes.boolean
      case SqlDbValue.IsNotNull(_)               => AnsiTypes.boolean
      case SqlDbValue.ValueCase(_, _, orElse)    => orElse.tpe
      case SqlDbValue.ConditionCase(_, orElse)   => orElse.tpe
      case SqlDbValue.Custom(_, _, tpe)          => tpe
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
        SqlDbValue
          .Function(SqlExpr.FunctionName.Count, Seq(many.unsafeAsDbValue.unsafeAsAnyDbVal), AnsiTypes.bigint)
          .lift

      inline def unsafeAsDbValue: DbValue[A] = many.asInstanceOf[DbValue[A]]

      def map[B](f: DbValue[A] => DbValue[B]): Many[B] = f(many.asInstanceOf[DbValue[A]]).asInstanceOf[DbValue[Any]]
  }
  extension [T](t: T)(using mr: MapRes[Many, T])
    def mapManyN[B](f: mr.K[DbValue] => DbValue[B]): Many[B] =
      f(mr.toK(t).asInstanceOf[mr.K[DbValue]]).asInstanceOf[Many[B]]

  extension [A](optVal: DbValue[Option[A]])
    @targetName("dbValOptIsEmpty") def isEmpty: DbValue[Boolean]     = SqlDbValue.IsNull(optVal).lift
    @targetName("dbValOptIsDefined") def isDefined: DbValue[Boolean] = SqlDbValue.IsNotNull(optVal).lift

    @targetName("dbValOptOrElse") def orElse(other: DbValue[Option[A]]): DbValue[Option[A]] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Coalesce,
          Seq(optVal.unsafeAsAnyDbVal, other.unsafeAsAnyDbVal),
          other.tpe
        )
        .lift

    @targetName("dbValOptGetOrElse") def getOrElse(other: DbValue[A]): DbValue[A] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Coalesce,
          Seq(optVal.unsafeAsAnyDbVal, other.unsafeAsAnyDbVal),
          other.tpe
        )
        .lift

  val Case: CaseCompanion
  type CaseCompanion <: SqlCaseCompanion
  trait SqlCaseCompanion {
    def apply[A](v: DbValue[A]): ValueCase0[A]
    def when[A](whenCond: DbValue[Boolean])(thenV: DbValue[A]): ConditionCase[A]
  }

  trait ValueCase0[A] {
    def when[B](whenV: DbValue[A])(thenV: DbValue[B]): ValueCase1[A, B]
  }
  trait ValueCase1[A, B] {
    def when(whenV: DbValue[A])(thenV: DbValue[B]): ValueCase1[A, B]
    def otherwise(elseV: DbValue[B]): DbValue[B]
  }
  trait ConditionCase[A] {
    def when(whenCond: DbValue[Boolean])(thenV: DbValue[A]): ConditionCase[A]
    def otherwise(elseV: DbValue[A]): DbValue[A]
  }

  trait DefaultSqlCaseCompanion extends SqlCaseCompanion {
    override def apply[A](v: DbValue[A]): ValueCase0[A] = DefaultSqlCaseCompanion.DefaultValueCase0(v)

    override def when[A](whenCond: DbValue[Boolean])(thenV: DbValue[A]): ConditionCase[A] =
      DefaultSqlCaseCompanion.DefaultConditionCase(IndexedSeq((whenCond, thenV)))
  }
  object DefaultSqlCaseCompanion {
    case class DefaultValueCase0[A](matchOn: DbValue[A]) extends ValueCase0[A] {
      override def when[B](whenV: DbValue[A])(thenV: DbValue[B]): ValueCase1[A, B] =
        DefaultValueCase1(matchOn, IndexedSeq((whenV, thenV)))
    }

    case class DefaultValueCase1[A, B](matchOn: DbValue[A], cases: IndexedSeq[(DbValue[A], DbValue[B])])
        extends ValueCase1[A, B] {
      override def when(whenV: DbValue[A])(thenV: DbValue[B]): ValueCase1[A, B] =
        DefaultValueCase1(matchOn, cases :+ ((whenV, thenV)))

      override def otherwise(elseV: DbValue[B]): DbValue[B] = SqlDbValue.ValueCase(matchOn, cases, elseV).lift
    }

    case class DefaultConditionCase[A](cases: IndexedSeq[(DbValue[Boolean], DbValue[A])]) extends ConditionCase[A] {
      override def when(whenCond: DbValue[Boolean])(thenV: DbValue[A]): ConditionCase[A] =
        DefaultConditionCase(cases :+ ((whenCond, thenV)))

      override def otherwise(elseV: DbValue[A]): DbValue[A] = SqlDbValue.ConditionCase(cases, elseV).lift
    }

  }
}
