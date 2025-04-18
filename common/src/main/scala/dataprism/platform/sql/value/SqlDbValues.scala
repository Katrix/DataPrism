package dataprism.platform.sql.value

import java.sql.{Date, Time, Timestamp}

import scala.annotation.targetName
import scala.util.NotGiven

import cats.data.State
import cats.syntax.all.*
import dataprism.platform.MapRes
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sharedast.SqlExpr
import dataprism.sql.*
import perspective.*

trait SqlDbValues extends SqlDbValuesBase { platform: SqlQueryPlatform =>

  type Impl <: SqlDbValueImpl & SqlBaseImpl
  trait SqlDbValueImpl extends SqlValuesBaseImpl {
    def asc[A](v: DbValue[A]): Ord
    def desc[A](v: DbValue[A]): Ord
    def unsafeAsAnyDbVal[A](v: DbValue[A]): AnyDbValue

    override def function[A](name: SqlExpr.FunctionName, args: Seq[AnyDbValue], tpe: Type[A]): DbValue[A] =
      SqlDbValue.Function(name, args, tpe).lift
    override def unaryOp[V, R](value: DbValue[V], unaryOp: UnaryOp[V, R]): DbValue[R] =
      SqlDbValue.UnaryOp(value, unaryOp).lift
    override def binaryOp[LHS, RHS, R](lhs: DbValue[LHS], rhs: DbValue[RHS], binaryOp: BinOp[LHS, RHS, R]): DbValue[R] =
      SqlDbValue.BinOp(lhs, rhs, binaryOp).lift

    override def nullableUnaryOp[V, R](
        op: UnaryOp[V, R]
    )(using ev1: NotGiven[SqlNull <:< V], ev2: NotGiven[SqlNull <:< R]): UnaryOp[V | SqlNull, R | SqlNull] =
      FundamentalUnaryOp.NullableOp(op)

    override def nullableBinOp[LHS, RHS, R](op: BinOp[LHS, RHS, R])(
        using NotGiven[SqlNull <:< LHS],
        NotGiven[SqlNull <:< RHS],
        NotGiven[SqlNull <:< R]
    ): BinOp[LHS | SqlNull, RHS | SqlNull, R | SqlNull] = FundamentalBinOp.NullableOp(op)
  }

  enum FundamentalUnaryOp[V, R](val name: String, op: SqlExpr.UnaryOperation) extends UnaryOp[V, R] {
    case Not[A](logic: SqlLogic[A]) extends FundamentalUnaryOp[A, A]("not", SqlExpr.UnaryOperation.Not)
    case Negative[A, SumResult, AvgResult](numeric: SqlNumeric[A /*, SumResult, AvgResult*/ ])
        extends FundamentalUnaryOp[A, A]("negation", SqlExpr.UnaryOperation.Negation)
    case NullableOp[V1, R1](op: UnaryOp[V1, R1]) extends FundamentalUnaryOp[V1 | SqlNull, R1 | SqlNull](op.name, op.ast)

    override def ast: SqlExpr.UnaryOperation = op

    override def tpe(v: DbValue[V]): Type[R] = this match
      case Not(logic)              => logic.tpe(v, v)
      case Negative(numeric)       => numeric.tpe(v, v)
      case nop: NullableOp[v1, r1] =>
        // GADT not playing nice
        nop.op.tpe(v.asInstanceOf[DbValue[v1 | SqlNull]].unsafeGet).nullable.asInstanceOf[Type[R]]

  }

  enum FundamentalBinOp[LHS, RHS, R](val name: String, op: SqlExpr.BinaryOperation) extends BinOp[LHS, RHS, R] {
    case Eq[A]()  extends FundamentalBinOp[A, A, Boolean]("eq", SqlExpr.BinaryOperation.Eq)
    case Neq[A]() extends FundamentalBinOp[A, A, Boolean]("neq", SqlExpr.BinaryOperation.Neq)

    case LessThan[A]()       extends FundamentalBinOp[A, A, Boolean]("lt", SqlExpr.BinaryOperation.LessThan)
    case LessOrEqual[A]()    extends FundamentalBinOp[A, A, Boolean]("le", SqlExpr.BinaryOperation.LessOrEq)
    case GreaterThan[A]()    extends FundamentalBinOp[A, A, Boolean]("gt", SqlExpr.BinaryOperation.GreaterThan)
    case GreaterOrEqual[A]() extends FundamentalBinOp[A, A, Boolean]("ge", SqlExpr.BinaryOperation.GreaterOrEq)

    case And[A](logic: SqlLogic[A]) extends FundamentalBinOp[A, A, A]("and", SqlExpr.BinaryOperation.BoolAnd)
    case Or[A](logic: SqlLogic[A])  extends FundamentalBinOp[A, A, A]("or", SqlExpr.BinaryOperation.BoolOr)

    case Plus[A, SumResult, AvgResult](numeric: SqlNumeric[A])
        extends FundamentalBinOp[A, A, A]("plus", SqlExpr.BinaryOperation.Plus)
    case Minus[A, SumResult, AvgResult](numeric: SqlNumeric[A])
        extends FundamentalBinOp[A, A, A]("minus", SqlExpr.BinaryOperation.Minus)
    case Multiply[A, SumResult, AvgResult](numeric: SqlNumeric[A])
        extends FundamentalBinOp[A, A, A]("times", SqlExpr.BinaryOperation.Multiply)
    case Divide[A, SumResult, AvgResult](numeric: SqlNumeric[A])
        extends FundamentalBinOp[A, A, Nullable[A]]("divide", SqlExpr.BinaryOperation.Divide)
    case Remainder[A, SumResult, AvgResult](numeric: SqlNumeric[A])
        extends FundamentalBinOp[A, A, A]("remainder", SqlExpr.BinaryOperation.Remainder)

    case NullableOp[LHS1, RHS1, R1](binop: BinOp[LHS1, RHS1, R1])
        extends FundamentalBinOp[LHS1 | SqlNull, RHS1 | SqlNull, R1 | SqlNull](binop.name, binop.ast)

    override def ast: SqlExpr.BinaryOperation = op

    override def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[R] = this match
      case Eq()  => AnsiTypes.boolean.notNull
      case Neq() => AnsiTypes.boolean.notNull

      case LessThan()       => AnsiTypes.boolean.notNull
      case LessOrEqual()    => AnsiTypes.boolean.notNull
      case GreaterThan()    => AnsiTypes.boolean.notNull
      case GreaterOrEqual() => AnsiTypes.boolean.notNull

      case And(logic) => logic.tpe(lhs, rhs)
      case Or(logic)  => logic.tpe(lhs, rhs)

      case Plus(numeric)     => numeric.tpe(lhs, rhs)
      case Minus(numeric)    => numeric.tpe(lhs, rhs)
      case Multiply(numeric) => numeric.tpe(lhs, rhs)
      case Divide(numeric)   =>
        // GADT not playing nice
        numeric.tpe(lhs, rhs).nullable.asInstanceOf[Type[R]]
      case Remainder(numeric) => numeric.tpe(lhs, rhs)

      case nop: NullableOp[lhs1, rhs1, r1] =>
        // GADT not playing nice
        nop.binop
          .tpe(
            lhs.asSome.asInstanceOf[DbValue[lhs1 | SqlNull]].unsafeGet,
            rhs.asSome.asInstanceOf[DbValue[rhs1 | SqlNull]].unsafeGet
          )
          .nullable
          .asInstanceOf[Type[R]]
    end tpe
  }

  trait SqlOrdered[A] extends SqlOrderedBase[A]:
    override def greatest(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Greatest, (head +: tail).map(_.asAnyDbVal), head.tpe).lift

    override def least(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Least, (head +: tail).map(_.asAnyDbVal), head.tpe).lift

    extension (lhs: DbValue[A])
      @targetName("lessThan") override def <(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.LessThan()).lift)
      @targetName("lessOrEqual") override def <=(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.LessOrEqual()).lift)
      @targetName("greaterOrEqual") override def >=(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.GreaterOrEqual()).lift)
      @targetName("greatherThan") override def >(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.GreaterThan()).lift)

    extension (lhs: Many[A])
      def min: DbValue[A | SqlNull] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Min,
            Seq(lhs.asAnyDbVal),
            lhs.unsafeAsDbValue.tpe.choice.nullable
          )
          .lift
          .asInstanceOf[DbValue[A | SqlNull]]
      def max: DbValue[A | SqlNull] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Max,
            Seq(lhs.asAnyDbVal),
            lhs.unsafeAsDbValue.tpe.choice.nullable
          )
          .lift
          .asInstanceOf[DbValue[A | SqlNull]]

  object SqlOrdered:
    def defaultInstance[A]: SqlOrdered[A] = new SqlOrdered[A] {}

  given sqlOrderedString: SqlOrdered[String]                    = SqlOrdered.defaultInstance
  given sqlOrderedOptString: SqlOrdered[String | SqlNull]       = SqlOrdered.defaultInstance
  given sqlOrderedDate: SqlOrdered[Date]                        = SqlOrdered.defaultInstance
  given sqlOrderedOptDate: SqlOrdered[Date | SqlNull]           = SqlOrdered.defaultInstance
  given sqlOrderedTime: SqlOrdered[Time]                        = SqlOrdered.defaultInstance
  given sqlOrderedOptTime: SqlOrdered[Time | SqlNull]           = SqlOrdered.defaultInstance
  given sqlOrderedTimestamp: SqlOrdered[Timestamp]              = SqlOrdered.defaultInstance
  given sqlOrderedOptTimestamp: SqlOrdered[Timestamp | SqlNull] = SqlOrdered.defaultInstance

  trait SqlNumeric[A] extends SqlNumericBase[A], SqlOrdered[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = rhs.tpe

    extension (lhs: DbValue[A])
      @targetName("plus") override def +(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.Plus(this)).lift
      @targetName("minus") override def -(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.Minus(this)).lift
      @targetName("times") override def *(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.Multiply(this)).lift
      @targetName("divide") override def /(rhs: DbValue[A]): DbValue[Nullable[A]] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.Divide(this)).lift
      @targetName("remainder") override def %(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.Remainder(this)).lift
      @targetName("negation") override def unary_- : DbValue[A] =
        SqlDbValue.UnaryOp(lhs, FundamentalUnaryOp.Negative(this)).lift

  object SqlNumeric:
    def defaultInstance[A]: SqlNumeric[A] = new SqlNumeric[A] {}

  trait SqlNumericSumAverage[A, SumResult, AvgResult] extends SqlNumeric[A]:
    protected def sumType(v: Type[Nullable[A]]): Type[SumResult | SqlNull]
    protected def avgType(v: Type[Nullable[A]]): Type[AvgResult | SqlNull]

    extension (lhs: Many[A])
      def avg: DbValue[AvgResult | SqlNull] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Avg,
            Seq(lhs.asAnyDbVal),
            avgType(lhs.unsafeAsDbValue.tpe.nullable)
          )
          .lift
      def sum: DbValue[SumResult | SqlNull] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Sum,
            Seq(lhs.asAnyDbVal),
            sumType(lhs.unsafeAsDbValue.tpe.nullable)
          )
          .lift

  object SqlNumericSumAverage:
    def defaultInstance[A, SumResult, AvgResult](
        sumType0: Type[Nullable[A]] => Type[SumResult | SqlNull],
        avgType0: Type[Nullable[A]] => Type[AvgResult | SqlNull]
    ): SqlNumericSumAverage[A, SumResult, AvgResult] =
      new SqlNumericSumAverage[A, SumResult, AvgResult] {
        override protected def sumType(v: Type[Nullable[A]]): Type[SumResult | SqlNull] = sumType0(v)

        override protected def avgType(v: Type[Nullable[A]]): Type[AvgResult | SqlNull] = avgType0(v)
      }

  trait SqlFractional[A] extends SqlNumeric[A], SqlFractionalBase[A]
  object SqlFractional:
    def defaultInstance[A]: SqlFractional[A] =
      new SqlFractional[A] {}

  trait SqlFractionalSumAverage[A, SumResult, AvgResult]
      extends SqlFractional[A],
        SqlNumericSumAverage[A, SumResult, AvgResult]
  object SqlFractionalSumAverage:
    def defaultInstance[A, SumResult, AvgResult](
        sumType0: Type[Nullable[A]] => Type[SumResult | SqlNull],
        avgType0: Type[Nullable[A]] => Type[AvgResult | SqlNull]
    ): SqlFractionalSumAverage[A, SumResult, AvgResult] =
      new SqlFractionalSumAverage[A, SumResult, AvgResult] {
        override protected def sumType(v: Type[Nullable[A]]): Type[SumResult | SqlNull] = sumType0(v)

        override protected def avgType(v: Type[Nullable[A]]): Type[AvgResult | SqlNull] = avgType0(v)
      }

  trait SqlIntegral[A] extends SqlNumeric[A], SqlIntegralBase[A]
  object SqlIntegral:
    def defaultInstance[A]: SqlIntegral[A] = new SqlIntegral[A] {}

  trait SqlIntegralSumAverage[A, SumResult, AvgResult]
      extends SqlIntegral[A],
        SqlNumericSumAverage[A, SumResult, AvgResult]
  object SqlIntegralSumAverage:
    def defaultInstance[A, SumResult, AvgResult](
        sumType0: Type[Nullable[A]] => Type[SumResult | SqlNull],
        avgType0: Type[Nullable[A]] => Type[AvgResult | SqlNull]
    ): SqlIntegralSumAverage[A, SumResult, AvgResult] =
      new SqlIntegralSumAverage[A, SumResult, AvgResult] {
        override protected def sumType(v: Type[Nullable[A]]): Type[SumResult | SqlNull] = sumType0(v)

        override protected def avgType(v: Type[Nullable[A]]): Type[AvgResult | SqlNull] = avgType0(v)
      }

  override given sqlNumericShort: SqlIntegralSumAverage[Short, Long, BigDecimal] =
    SqlIntegralSumAverage.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericOptShort: SqlIntegralSumAverage[Short | SqlNull, Long, BigDecimal] =
    SqlIntegralSumAverage.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericInt: SqlIntegralSumAverage[Int, Long, BigDecimal] =
    SqlIntegralSumAverage.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericOptInt: SqlIntegralSumAverage[Int | SqlNull, Long, BigDecimal] =
    SqlIntegralSumAverage.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericLong: SqlIntegralSumAverage[Long, BigDecimal, BigDecimal] =
    SqlIntegralSumAverage.defaultInstance(_ => AnsiTypes.decimal.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericOptLong: SqlIntegralSumAverage[Long | SqlNull, BigDecimal, BigDecimal] =
    SqlIntegralSumAverage.defaultInstance(_ => AnsiTypes.decimal.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericFloat: SqlFractionalSumAverage[Float, Double, Double] =
    SqlFractionalSumAverage.defaultInstance(
      _ => AnsiTypes.doublePrecision.nullable,
      _ => AnsiTypes.doublePrecision.nullable
    )
  override given sqlNumericOptFloat: SqlFractionalSumAverage[Float | SqlNull, Double, Double] =
    SqlFractionalSumAverage.defaultInstance(
      _ => AnsiTypes.doublePrecision.nullable,
      _ => AnsiTypes.doublePrecision.nullable
    )
  override given sqlNumericDouble: SqlFractionalSumAverage[Double, Double, Double] =
    SqlFractionalSumAverage.defaultInstance(
      _ => AnsiTypes.doublePrecision.nullable,
      _ => AnsiTypes.doublePrecision.nullable
    )
  override given sqlNumericOptDouble: SqlFractionalSumAverage[Double | SqlNull, Double, Double] =
    SqlFractionalSumAverage.defaultInstance(
      _ => AnsiTypes.doublePrecision.nullable,
      _ => AnsiTypes.doublePrecision.nullable
    )
  override given sqlNumericBigDecimal: SqlFractionalSumAverage[BigDecimal, BigDecimal, BigDecimal] =
    SqlFractionalSumAverage.defaultInstance(identity, identity)
  override given sqlNumericOptBigDecimal: SqlFractionalSumAverage[BigDecimal | SqlNull, BigDecimal, BigDecimal] =
    SqlFractionalSumAverage.defaultInstance(identity, identity)

  trait SqlDbValueBaseImpl[A] extends SqlDbValueBase[A] {

    @targetName("dbEquals")
    override def ===(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.BinOp(n.castDbVal(this.liftDbValue), n.castDbVal(rhs), n.wrapBinOp(FundamentalBinOp.Eq())).lift

    @targetName("dbNotEquals")
    override def !==(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.BinOp(n.castDbVal(this.liftDbValue), n.castDbVal(rhs), n.wrapBinOp(FundamentalBinOp.Neq())).lift

    @targetName("dbValCast") override def cast[B](tpe: CastType[B])(using n: Nullability[A]): DbValue[n.N[B]] =
      SqlDbValue.Cast(this.asAnyDbVal, tpe.castTypeName, n.wrapType(tpe.castTypeType)).lift

    @targetName("dbValAsSome") override def asSome: DbValue[A | SqlNull] =
      SqlDbValue.AsSome(this.liftDbValue).lift

    @targetName("dbValInValues") override def in(head: DbValue[A], tail: DbValue[A]*)(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      SqlDbValue.InValues(this.liftDbValue, head +: tail, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNotInValues") override def notIn(head: DbValue[A], tail: DbValue[A]*)(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      SqlDbValue.NotInValues(this.liftDbValue, head +: tail, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValInQuery") override def in(query: Query[IdFC[A]])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.InQuery(this.liftDbValue, query, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNotInQuery") override def notIn(query: Query[IdFC[A]])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      SqlDbValue.NotInQuery(this.liftDbValue, query, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValInAsSeq") override def inAs(values: Seq[A], tpe: Type[A])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      if values.isEmpty then
        if n.isNullable then
          this.liftDbValue
            .asInstanceOf[DbValue[A | SqlNull]]
            .map(_ => DbValue.falseV)
            .asInstanceOf[DbValue[n.N[Boolean]]]
        else n.wrapDbVal(DbValue.falseV)
      else SqlDbValue.InValues(this.liftDbValue, values.map(_.as(tpe)), n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNotInAsSeq") override def notInAs(values: Seq[A], tpe: Type[A])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      if values.isEmpty then
        if n.isNullable then
          this.liftDbValue
            .asInstanceOf[DbValue[A | SqlNull]]
            .map(_ => DbValue.trueV)
            .asInstanceOf[DbValue[n.N[Boolean]]]
        else n.wrapDbVal(DbValue.trueV)
      else SqlDbValue.NotInValues(this.liftDbValue, values.map(_.as(tpe)), n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNullIf") override def nullIf(arg: DbValue[A]): DbValue[Nullable[A]] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.NullIf,
          Seq(this.asAnyDbVal, arg.asAnyDbVal),
          arg.tpe.nullable
        )
        .lift
        .asInstanceOf[DbValue[Nullable[A]]]

    def asSqlDbVal: Option[SqlDbValue[A]]

    def unsafeDbValAsMany: Many[A] = this.liftDbValue.asInstanceOf[Many[A]]
  }

  trait SqlDbValueCompanionImpl extends SqlDbValueCompanion {
    def rawK[A[_[_]]: TraverseKC, B](args: A[DbValue], tpe: Type[B])(
        render: A[Const[SqlStr[Codec]]] => SqlStr[Codec]
    ): DbValue[B] =
      val argsList: List[AnyDbValue] = args.foldMapK([X] => (v: DbValue[X]) => List(v.asAnyDbVal))
      val indicesState: State[Int, A[Const[Int]]] =
        args.traverseK([X] => (_: DbValue[X]) => State((acc: Int) => (acc + 1, acc)))
      val indices: A[Const[Int]] = indicesState.runA(0).value
      SqlDbValue.Custom(argsList, strArgs => render(indices.mapConst([X] => (i: Int) => strArgs(i))), tpe).lift

    override def functionK[A[_[_]]: TraverseKC, B](name: String, tpe: Type[B])(args: A[DbValue]): DbValue[B] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Custom(name),
          args.foldMapK([X] => (v: DbValue[X]) => List(v.asAnyDbVal)),
          tpe
        )
        .lift

    override def nullV[A](tpe: Type[A]): DbValue[A | SqlNull] =
      SqlDbValue.Null(tpe).lift

    override def trueV: DbValue[Boolean]  = SqlDbValue.True.lift
    override def falseV: DbValue[Boolean] = SqlDbValue.False.lift
  }

  enum SqlDbValue[A] extends SqlDbValueBaseImpl[A] {
    case QueryColumn(queryName: String, fromName: String, override val tpe: Type[A])

    case JoinNullable[B](value: DbValue[B]) extends SqlDbValue[Nullable[B]]

    case UnaryOp[B, R](value: DbValue[B], op: platform.UnaryOp[B, R])                  extends SqlDbValue[R]
    case BinOp[B, C, R](lhs: DbValue[B], rhs: DbValue[C], op: platform.BinOp[B, C, R]) extends SqlDbValue[R]

    case Function(name: SqlExpr.FunctionName, values: Seq[AnyDbValue], override val tpe: Type[A])
    case Cast(value: AnyDbValue, typeName: String, override val tpe: Type[A])

    case GetNullable(value: DbValue[A | SqlNull])
    case AsSome[B](value: DbValue[B]) extends SqlDbValue[B | SqlNull]

    case Placeholder(valueSeq: Seq[A], override val tpe: Type[A])
    case CompilePlaceholder(identifier: Object, override val tpe: Type[A])

    case SubSelect(query: Query[IdFC[A]])

    case Null[B](baseTpe: Type[B])                 extends SqlDbValue[B | SqlNull]
    case IsNull[B](value: DbValue[B | SqlNull])    extends SqlDbValue[Boolean]
    case IsNotNull[B](value: DbValue[B | SqlNull]) extends SqlDbValue[Boolean]

    case InValues[B, R](v: DbValue[B], values: Seq[DbValue[B]], override val tpe: Type[R])    extends SqlDbValue[R]
    case NotInValues[B, R](v: DbValue[B], values: Seq[DbValue[B]], override val tpe: Type[R]) extends SqlDbValue[R]
    case InQuery[B, R](v: DbValue[B], query: Query[IdFC[B]], override val tpe: Type[R])       extends SqlDbValue[R]
    case NotInQuery[B, R](v: DbValue[B], query: Query[IdFC[B]], override val tpe: Type[R])    extends SqlDbValue[R]

    case ValueCase[V, R](matchOn: DbValue[V], cases: IndexedSeq[(DbValue[V], DbValue[R])], orElse: DbValue[R])
        extends SqlDbValue[R]
    case ConditionCase(cases: IndexedSeq[(DbValue[Boolean], DbValue[A])], orElse: DbValue[A])

    case Custom(args: Seq[AnyDbValue], render: Seq[SqlStr[Codec]] => SqlStr[Codec], override val tpe: Type[A])
    case QueryCount extends SqlDbValue[Long]
    case True       extends SqlDbValue[Boolean]
    case False      extends SqlDbValue[Boolean]

    override def ast: TagState[SqlExpr[Codec]] = this match
      case SqlDbValue.QueryColumn(queryName, fromName, _) => State.pure(SqlExpr.QueryRef(fromName, queryName))

      case SqlDbValue.UnaryOp(value, op) => value.ast.map(v => SqlExpr.UnaryOp(v, op.ast, op.tpe(value).name))
      case SqlDbValue.BinOp(lhs, rhs, op) =>
        lhs.ast.flatMap(l => rhs.ast.map(r => SqlExpr.BinOp(l, r, op.ast, op.tpe(lhs, rhs).name)))

      case SqlDbValue.JoinNullable(value) => value.ast
      case SqlDbValue.Function(f, values, tpe) =>
        values.toList.traverse(_.ast).map(exprs => SqlExpr.FunctionCall(f, exprs, tpe.name))
      case SqlDbValue.Cast(value, typeName, _) => value.ast.map(v => SqlExpr.Cast(v, typeName))

      case SqlDbValue.GetNullable(value) => value.ast
      case SqlDbValue.AsSome(value)      => value.ast

      case SqlDbValue.Placeholder(value, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.SqlArgObj(value, tpe.codec)))

      case SqlDbValue.CompilePlaceholder(identifier, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.CompileArg(identifier, tpe.codec)))

        State.pure(SqlExpr.PreparedArgument(None, SqlArg.CompileArg(identifier, tpe.codec)))

      case SqlDbValue.SubSelect(query) => query.selectAstAndValues.map(m => SqlExpr.SubSelect(m.ast))

      case SqlDbValue.Null(_)          => State.pure(SqlExpr.Null())
      case SqlDbValue.IsNull(value)    => value.ast.map(v => SqlExpr.IsNull(v))
      case SqlDbValue.IsNotNull(value) => value.ast.map(v => SqlExpr.IsNotNull(v))

      case SqlDbValue.InValues(v, values, _) =>
        (v.ast, values.traverse(_.ast)).mapN((v, vs) => SqlExpr.InValues(v, vs))
      case SqlDbValue.NotInValues(v, values, _) =>
        (v.ast, values.traverse(_.ast)).mapN((v, vs) => SqlExpr.NotInValues(v, vs))
      case SqlDbValue.InQuery(v, query, _) =>
        (v.ast, query.selectAstAndValues).mapN((v, m) => SqlExpr.InQuery(v, m.ast))
      case SqlDbValue.NotInQuery(v, query, _) =>
        (v.ast, query.selectAstAndValues).mapN((v, m) => SqlExpr.NotInQuery(v, m.ast))

      case SqlDbValue.ValueCase(matchOn, cases, orElse) =>
        (
          matchOn.ast,
          cases.toVector.traverse(c => (c._1.ast, c._2.ast).tupled),
          orElse.ast
        ).mapN((m, cs, o) => SqlExpr.ValueCase(m, cs, o))
      case SqlDbValue.ConditionCase(cases, orElse) =>
        (
          cases.toVector.traverse(c => (c._1.ast, c._2.ast).tupled),
          orElse.ast
        ).mapN((cs, o) => SqlExpr.ConditionCase(cs, o))

      case SqlDbValue.Custom(args, render, _) =>
        args.toVector.traverse(_.ast).map(exprs => SqlExpr.Custom(exprs, render))
      case SqlDbValue.QueryCount => State.pure(SqlExpr.QueryCount())
      case SqlDbValue.True       => State.pure(SqlExpr.True())
      case SqlDbValue.False      => State.pure(SqlExpr.False())
    end ast

    override def asSqlDbVal: Option[SqlDbValue[A]] = Some(this)

    override def tpe: Type[A] = this match
      case SqlDbValue.QueryColumn(_, _, tpe)     => tpe
      case SqlDbValue.UnaryOp(v, op)             => op.tpe(v)
      case SqlDbValue.BinOp(lhs, rhs, op)        => op.tpe(lhs, rhs)
      case SqlDbValue.JoinNullable(value)        => value.tpe.choice.nullable.asInstanceOf[Type[A]]
      case SqlDbValue.Function(_, _, tpe)        => tpe
      case SqlDbValue.Cast(_, _, tpe)            => tpe
      case SqlDbValue.GetNullable(value)         => value.tpe.nullableChoice.notNull.asInstanceOf[Type[A]]
      case SqlDbValue.AsSome(value)              => value.tpe.choice.nullable.asInstanceOf[Type[A]]
      case SqlDbValue.Placeholder(_, tpe)        => tpe
      case SqlDbValue.CompilePlaceholder(_, tpe) => tpe
      case SqlDbValue.SubSelect(query)           => query.selectAstAndValues.runA(freshTaggedState).value.values.tpe
      case SqlDbValue.Null(tpe)                  => tpe.nullable.asInstanceOf[Type[A]]
      case SqlDbValue.IsNull(_)                  => AnsiTypes.boolean.notNull
      case SqlDbValue.IsNotNull(_)               => AnsiTypes.boolean.notNull
      case SqlDbValue.InValues(_, _, tpe)        => tpe
      case SqlDbValue.NotInValues(_, _, tpe)     => tpe
      case SqlDbValue.InQuery(_, _, tpe)         => tpe
      case SqlDbValue.NotInQuery(_, _, tpe)      => tpe
      case SqlDbValue.ValueCase(_, _, orElse)    => orElse.tpe
      case SqlDbValue.ConditionCase(_, orElse)   => orElse.tpe
      case SqlDbValue.Custom(_, _, tpe)          => tpe
      case SqlDbValue.QueryCount                 => AnsiTypes.bigint.notNull
      case SqlDbValue.True                       => AnsiTypes.boolean.notNull
      case SqlDbValue.False                      => AnsiTypes.boolean.notNull
    end tpe

    override def columnName(prefix: String): String = this match
      case SqlDbValue.QueryColumn(queryName, _, _) =>
        if queryName.startsWith("x") then s"${prefix}_${queryName.drop(1).dropWhile(c => c.isDigit || c == '_')}"
        else s"${prefix}_$queryName"

      case SqlDbValue.UnaryOp(v, op) => s"${v.columnName(prefix)}_${op.name}"
      case SqlDbValue.BinOp(lhs, rhs, op) =>
        s"${lhs.columnName(prefix)}_${rhs.columnName(prefix).substring(prefix.length)}_${op.name}"
      case SqlDbValue.JoinNullable(value) => value.columnName(prefix)
      case SqlDbValue.Function(name, values, _) =>
        s"${values.headOption.fold(prefix)(_.columnName(prefix))}_${name.name}"
      case SqlDbValue.Cast(v, _, _)            => s"${v.columnName(prefix)}_cast"
      case SqlDbValue.GetNullable(value)       => value.columnName(prefix)
      case SqlDbValue.AsSome(value)            => value.columnName(prefix)
      case SqlDbValue.Placeholder(_, _)        => prefix
      case SqlDbValue.CompilePlaceholder(_, _) => prefix
      case SqlDbValue.SubSelect(query) =>
        query.selectAstAndValues.runA(freshTaggedState).value.values.columnName(prefix)
      case SqlDbValue.Null(_)              => s"${prefix}_null"
      case SqlDbValue.IsNull(v)            => s"${v.columnName(prefix)}_is_null"
      case SqlDbValue.IsNotNull(v)         => s"${v.columnName(prefix)}_is_not_null"
      case SqlDbValue.InValues(v, _, _)    => s"${v.columnName(prefix)}_in"
      case SqlDbValue.NotInValues(v, _, _) => s"${v.columnName(prefix)}_in"
      case SqlDbValue.InQuery(v, _, _)     => s"${v.columnName(prefix)}_in"
      case SqlDbValue.NotInQuery(v, _, _)  => s"${v.columnName(prefix)}_in"
      case SqlDbValue.ValueCase(v, _, _)   => s"${v.columnName(prefix)}_case"
      case SqlDbValue.ConditionCase(_, _)  => prefix
      case SqlDbValue.Custom(_, _, _)      => prefix
      case SqlDbValue.QueryCount           => s"${prefix}_count"
      case SqlDbValue.True                 => s"${prefix}_true"
      case SqlDbValue.False                => s"${prefix}_false"
    end columnName

    override def asAnyDbVal: AnyDbValue = Impl.unsafeAsAnyDbVal(this.lift)

    override protected def liftDbValue: DbValue[A] = this.lift

    override def asc: Ord = Impl.asc(this.lift)

    override def desc: Ord = Impl.desc(this.lift)
  }

  given [A]: Lift[SqlDbValue[A], DbValue[A]] = sqlDbValueLift[A]
  protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]]

  extension [A](v: A)
    @targetName("valueAs") override def as(tpe: Type[A]): DbValue[A] = SqlDbValue.Placeholder(Seq(v), tpe).lift

  trait SqlLogic[A] extends SqlLogicBase[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = rhs.tpe

    extension (lhs: DbValue[A])
      @targetName("and") def &&(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.And(this)).lift

      @targetName("or") def ||(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, FundamentalBinOp.Or(this)).lift

      @targetName("not") def unary_! : DbValue[A] =
        SqlDbValue.UnaryOp(lhs, FundamentalUnaryOp.Not(this)).lift
  object SqlLogic:
    def defaultInstance[A]: SqlLogic[A] = new SqlLogic[A] {}

  given booleanSqlLogic: SqlLogic[Boolean]              = SqlLogic.defaultInstance
  given booleanOptSqlLogic: SqlLogic[Boolean | SqlNull] = SqlLogic.defaultInstance

  opaque type Many[A] = DbValue[A]
  object Many extends ManyCompanion {
    extension [A](many: Many[A])
      // TODO: Check that the return type is indeed Long on all platforms
      def count: DbValue[Long] =
        SqlDbValue
          .Function(SqlExpr.FunctionName.Count, Seq(many.unsafeAsDbValue.asAnyDbVal), AnsiTypes.bigint.notNull)
          .lift

      def unsafeAsDbValue: DbValue[A] = many

      def map[B](f: DbValue[A] => DbValue[B]): Many[B] = f(many)
  }
  extension [T](t: T)(using mr: MapRes[Many, T]) def mapManyN[B](f: mr.K[DbValue] => DbValue[B]): Many[B] = f(mr.toK(t))

  extension [A](optVal: DbValue[A | SqlNull])(using NotGiven[SqlNull <:< A])
    @targetName("dbValOptgetUnsafe") def unsafeGet: DbValue[A] =
      SqlDbValue.GetNullable(optVal).lift

    @targetName("dbValOptMap") def map[B](f: DbValue[A] => DbValue[B]): DbValue[B | SqlNull] =
      Case.when(optVal.isDefined)(f(optVal.unsafeGet).asSome).otherwise(DbValue.nullV(f(optVal.unsafeGet).tpe))

    @targetName("dbValOptFilter") def filter(f: DbValue[A] => DbValue[Boolean]): DbValue[A | SqlNull] =
      Case.when(optVal.isDefined && f(optVal.unsafeGet))(optVal).otherwise(DbValue.nullV(optVal.tpe))

    @targetName("dbValOptFlatMap") def flatMap[B](f: DbValue[A] => DbValue[B | SqlNull]): DbValue[B | SqlNull] =
      Case.when(optVal.isDefined)(f(optVal.unsafeGet)).otherwise(DbValue.nullV(f(optVal.unsafeGet).unsafeGet.tpe))

    @targetName("dbValOptIsEmpty") def isEmpty: DbValue[Boolean]     = SqlDbValue.IsNull(optVal).lift
    @targetName("dbValOptIsDefined") def isDefined: DbValue[Boolean] = SqlDbValue.IsNotNull(optVal).lift

    @targetName("dbValOptOrElse") def orElse(other: DbValue[A | SqlNull]): DbValue[A | SqlNull] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Coalesce,
          Seq(optVal.asAnyDbVal, other.asAnyDbVal),
          other.tpe
        )
        .lift

    @targetName("dbValOptGetOrElse") def getOrElse(other: DbValue[A]): DbValue[A] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Coalesce,
          Seq(optVal.asAnyDbVal, other.asAnyDbVal),
          other.tpe
        )
        .lift

  extension [T](t: T)(using mr: MapRes[Compose2[DbValue, Nullable], T])
    def mapNullableN[B](f: mr.K[DbValue] => DbValue[B]): DbValue[B | SqlNull] =
      given ApplyKC[mr.K]    = mr.applyKC
      given FoldableKC[mr.K] = mr.traverseKC
      val res                = f(mr.toK(t).mapK([Z] => (v: DbValue[Z | SqlNull]) => v.unsafeGet))
      Case
        .when(mr.toK(t).foldLeftK(DbValue.trueV)(acc => [Z] => (v: DbValue[Z | SqlNull]) => acc && v.isDefined))(
          res.asSome
        )
        .otherwise(DbValue.nullV(res.tpe))

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

  type Api <: SqlDbValueImplApi & SqlDbValueApi & QueryApi
  trait SqlDbValueImplApi {
    export platform.{SqlFractionalSumAverage, SqlIntegralSumAverage, SqlNumericSumAverage}

    inline def SqlOrdered: platform.SqlOrdered.type       = platform.SqlOrdered
    inline def SqlNumeric: platform.SqlNumeric.type       = platform.SqlNumeric
    inline def SqlIntegral: platform.SqlIntegral.type     = platform.SqlIntegral
    inline def SqlFractional: platform.SqlFractional.type = platform.SqlFractional
  }
}
