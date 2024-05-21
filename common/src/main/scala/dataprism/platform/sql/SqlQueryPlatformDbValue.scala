package dataprism.platform.sql

import java.sql.{Date, Time, Timestamp}

import scala.annotation.targetName
import scala.util.NotGiven

import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.sharedast.SqlExpr
import dataprism.sql.*
import perspective.*

trait SqlQueryPlatformDbValue extends SqlQueryPlatformDbValueBase { platform: SqlQueryPlatform =>

  type Impl <: SqlDbValueImpl & SqlBaseImpl
  trait SqlDbValueImpl {
    def asc[A](v: DbValue[A]): Ord
    def desc[A](v: DbValue[A]): Ord
    def unsafeAsAnyDbVal[A](v: DbValue[A]): AnyDbValue
  }

  enum SqlUnaryOp[V, R](val name: String, op: SqlExpr.UnaryOperation) extends SqlUnaryOpBase[V, R] {
    case Not[A](logic: SqlLogic[A])              extends SqlUnaryOp[A, A]("not", SqlExpr.UnaryOperation.Not)
    case Negative[A](numeric: SqlNumeric[A])     extends SqlUnaryOp[A, A]("negation", SqlExpr.UnaryOperation.Negation)
    case NullableOp[V1, R1](op: UnaryOp[V1, R1]) extends SqlUnaryOp[Option[V1], Option[R1]](op.name, op.ast)

    override def ast: SqlExpr.UnaryOperation = op

    override def tpe(v: DbValue[V]): Type[R] = this match
      case Not(logic)        => logic.tpe(v, v)
      case Negative(numeric) => numeric.tpe(v, v)
      case nop: NullableOp[v1, r1] =>
        given ev1: (DbValue[V] =:= DbValue[Option[v1]]) = <:<.refl
        given ev2: (Type[Option[r1]] =:= Type[R])       = <:<.refl

        ev2(nop.op.tpe(ev1(v).unsafeGet).typedChoice.nullable)

    override def nullable(using NotGiven[V <:< Option[_]], NotGiven[R <:< Option[_]]): UnaryOp[Option[V], Option[R]] =
      SqlUnaryOp.NullableOp(this.liftSqlUnaryOp).liftSqlUnaryOp
  }

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R]

  enum SqlBinOp[LHS, RHS, R](val name: String, op: SqlExpr.BinaryOperation) extends SqlBinOpBase[LHS, RHS, R] {
    case Eq[A]()  extends SqlBinOp[A, A, Boolean]("eq", SqlExpr.BinaryOperation.Eq)
    case Neq[A]() extends SqlBinOp[A, A, Boolean]("neq", SqlExpr.BinaryOperation.Neq)

    case LessThan[A]()       extends SqlBinOp[A, A, Boolean]("lt", SqlExpr.BinaryOperation.LessThan)
    case LessOrEqual[A]()    extends SqlBinOp[A, A, Boolean]("le", SqlExpr.BinaryOperation.LessOrEq)
    case GreaterThan[A]()    extends SqlBinOp[A, A, Boolean]("gt", SqlExpr.BinaryOperation.GreaterThan)
    case GreaterOrEqual[A]() extends SqlBinOp[A, A, Boolean]("ge", SqlExpr.BinaryOperation.GreaterOrEq)

    case And[A](logic: SqlLogic[A]) extends SqlBinOp[A, A, A]("and", SqlExpr.BinaryOperation.BoolAnd)
    case Or[A](logic: SqlLogic[A])  extends SqlBinOp[A, A, A]("or", SqlExpr.BinaryOperation.BoolOr)

    case Plus[A](numeric: SqlNumeric[A])     extends SqlBinOp[A, A, A]("plus", SqlExpr.BinaryOperation.Plus)
    case Minus[A](numeric: SqlNumeric[A])    extends SqlBinOp[A, A, A]("minus", SqlExpr.BinaryOperation.Minus)
    case Multiply[A](numeric: SqlNumeric[A]) extends SqlBinOp[A, A, A]("times", SqlExpr.BinaryOperation.Multiply)
    case Divide[A](numeric: SqlNumeric[A]) extends SqlBinOp[A, A, Nullable[A]]("divide", SqlExpr.BinaryOperation.Divide)
    case Remainder[A](numeric: SqlNumeric[A]) extends SqlBinOp[A, A, A]("remainder", SqlExpr.BinaryOperation.Remainder)

    case NullableOp[LHS1, RHS1, R1](binop: BinOp[LHS1, RHS1, R1])
        extends SqlBinOp[Option[LHS1], Option[RHS1], Option[R1]](binop.name, binop.ast)

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

      case Plus(numeric)      => numeric.tpe(lhs, rhs)
      case Minus(numeric)     => numeric.tpe(lhs, rhs)
      case Multiply(numeric)  => numeric.tpe(lhs, rhs)
      case Divide(numeric)    => numeric.tpe(lhs, rhs).typedChoice.nullable.asInstanceOf[Type[R]]
      case Remainder(numeric) => numeric.tpe(lhs, rhs)

      case nop: NullableOp[lhs1, rhs1, r1] =>
        given ev1: (DbValue[LHS] =:= DbValue[Option[lhs1]]) = <:<.refl
        given ev2: (DbValue[RHS] =:= DbValue[Option[rhs1]]) = <:<.refl
        given ev3: (Type[Option[r1]] =:= Type[R])           = <:<.refl

        ev3(nop.binop.tpe(ev1(lhs).unsafeGet, ev2(rhs).unsafeGet).typedChoice.nullable)
    end tpe

    override def nullable(
        using NotGiven[LHS <:< Option[?]],
        NotGiven[RHS <:< Option[?]],
        NotGiven[R <:< Option[?]]
    ): BinOp[Option[LHS], Option[RHS], Option[R]] =
      NullableOp(this.liftSqlBinOp).liftSqlBinOp
  }

  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait SqlOrdered[A](using n0: NullabilityOf[A]) extends SqlOrderedBase[A]:
    override def greatest(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Greatest, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    override def least(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Least, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    extension (lhs: DbValue[A])
      @targetName("lessThan") override def <(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessThan().liftSqlBinOp).lift)
      @targetName("lessOrEqual") override def <=(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessOrEqual().liftSqlBinOp).lift)
      @targetName("greaterOrEqual") override def >=(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterOrEqual().liftSqlBinOp).lift)
      @targetName("greatherThan") override def >(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
        n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterThan().liftSqlBinOp).lift)

    extension (lhs: Many[A])
      def min: DbValue[Nullable[A]] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Min,
            Seq(lhs.unsafeAsAnyDbVal),
            lhs.unsafeAsDbValue.tpe.typedChoice.nullable
          )
          .lift
          .asInstanceOf[DbValue[Nullable[A]]]
      def max: DbValue[Nullable[A]] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Max,
            Seq(lhs.unsafeAsAnyDbVal),
            lhs.unsafeAsDbValue.tpe.typedChoice.nullable
          )
          .lift
          .asInstanceOf[DbValue[Nullable[A]]]

  object SqlOrdered:
    def defaultInstance[A](using na: NullabilityOf[A]): SqlOrdered[A] = new SqlOrdered[A] {}

  given sqlOrderedString: SqlOrdered[String]                  = SqlOrdered.defaultInstance
  given sqlOrderedOptString: SqlOrdered[Option[String]]       = SqlOrdered.defaultInstance
  given sqlOrderedDate: SqlOrdered[Date]                      = SqlOrdered.defaultInstance
  given sqlOrderedOptDate: SqlOrdered[Option[Date]]           = SqlOrdered.defaultInstance
  given sqlOrderedTime: SqlOrdered[Time]                      = SqlOrdered.defaultInstance
  given sqlOrderedOptTime: SqlOrdered[Option[Time]]           = SqlOrdered.defaultInstance
  given sqlOrderedTimestamp: SqlOrdered[Timestamp]            = SqlOrdered.defaultInstance
  given sqlOrderedOptTimestamp: SqlOrdered[Option[Timestamp]] = SqlOrdered.defaultInstance

  trait SqlNumeric[A] extends SqlNumericBase[A], SqlOrdered[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = rhs.tpe

    extension (lhs: DbValue[A])
      @targetName("plus") override def +(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Plus(this).liftSqlBinOp).lift
      @targetName("minus") override def -(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Minus(this).liftSqlBinOp).lift
      @targetName("times") override def *(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Multiply(this).liftSqlBinOp).lift
      @targetName("divide") override def /(rhs: DbValue[A]): DbValue[Nullable[A]] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Divide(this).liftSqlBinOp).lift
      @targetName("remainder") override def %(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Remainder(this).liftSqlBinOp).lift
      @targetName("negation") override def unary_- : DbValue[A] =
        SqlDbValue.UnaryOp(lhs, SqlUnaryOp.Negative(this).liftSqlUnaryOp).lift

    protected def sumType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.SumResultOf[A]]]
    protected def avgType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.AvgResultOf[A]]]

    extension (lhs: Many[A])
      // TODO: Having these in here is quite broad. Might want to tighten this
      def avg: DbValue[Nullable[SqlNumeric.AvgResultOf[A]]] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Avg,
            Seq(lhs.unsafeAsAnyDbVal),
            avgType(lhs.unsafeAsDbValue.tpe.typedChoice.nullable.asInstanceOf[Type[Nullable[A]]])
          )
          .lift
      def sum: DbValue[Nullable[SqlNumeric.SumResultOf[A]]] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Sum,
            Seq(lhs.unsafeAsAnyDbVal),
            sumType(lhs.unsafeAsDbValue.tpe.typedChoice.nullable.asInstanceOf[Type[Nullable[A]]])
          )
          .lift

  object SqlNumeric:
    type SumResultOf[T] = T match {
      case Short | Int    => Long
      case Long           => BigDecimal
      case Float | Double => Double
      case Option[a]      => SumResultOf[a]
      case _              => T
    }
    type AvgResultOf[T] = T match {
      case Short | Int | Long => BigDecimal
      case Float | Double     => Double
      case Option[a]          => AvgResultOf[a]
      case _                  => T
    }

    def defaultInstance[A](
        sumType0: Type[Nullable[A]] => Type[Nullable[SqlNumeric.SumResultOf[A]]],
        avgType0: Type[Nullable[A]] => Type[Nullable[SqlNumeric.AvgResultOf[A]]]
    )(using NullabilityOf[A]): SqlNumeric[A] =
      new SqlNumeric[A] {
        override protected def sumType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.SumResultOf[A]]] = sumType0(v)

        override protected def avgType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.AvgResultOf[A]]] = avgType0(v)
      }

  trait SqlFractional[A] extends SqlNumeric[A], SqlFractionalBase[A]
  object SqlFractional:
    def defaultInstance[A](
        sumType0: Type[Nullable[A]] => Type[Nullable[SqlNumeric.SumResultOf[A]]],
        avgType0: Type[Nullable[A]] => Type[Nullable[SqlNumeric.AvgResultOf[A]]]
    )(using NullabilityOf[A]): SqlFractional[A] =
      new SqlFractional[A] {
        override protected def sumType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.SumResultOf[A]]] = sumType0(v)

        override protected def avgType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.AvgResultOf[A]]] = avgType0(v)
      }

  trait SqlIntegral[A] extends SqlNumeric[A], SqlIntegralBase[A]
  object SqlIntegral:
    def defaultInstance[A](
        sumType0: Type[Nullable[A]] => Type[Nullable[SqlNumeric.SumResultOf[A]]],
        avgType0: Type[Nullable[A]] => Type[Nullable[SqlNumeric.AvgResultOf[A]]]
    )(using NullabilityOf[A]): SqlIntegral[A] =
      new SqlIntegral[A] {
        override protected def sumType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.SumResultOf[A]]] = sumType0(v)

        override protected def avgType(v: Type[Nullable[A]]): Type[Nullable[SqlNumeric.AvgResultOf[A]]] = avgType0(v)
      }

  override given sqlNumericShort: SqlIntegral[Short] =
    SqlIntegral.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericOptShort: SqlIntegral[Option[Short]] =
    SqlIntegral.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericInt: SqlIntegral[Int] =
    SqlIntegral.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericOptInt: SqlIntegral[Option[Int]] =
    SqlIntegral.defaultInstance(_ => AnsiTypes.bigint.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericLong: SqlIntegral[Long] =
    SqlIntegral.defaultInstance(_ => AnsiTypes.decimal.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericOptLong: SqlIntegral[Option[Long]] =
    SqlIntegral.defaultInstance(_ => AnsiTypes.decimal.nullable, _ => AnsiTypes.decimal.nullable)
  override given sqlNumericFloat: SqlFractional[Float] =
    SqlFractional.defaultInstance(_ => AnsiTypes.doublePrecision.nullable, _ => AnsiTypes.doublePrecision.nullable)
  override given sqlNumericOptFloat: SqlFractional[Option[Float]] =
    SqlFractional.defaultInstance(_ => AnsiTypes.doublePrecision.nullable, _ => AnsiTypes.doublePrecision.nullable)
  override given sqlNumericDouble: SqlFractional[Double] =
    SqlFractional.defaultInstance(_ => AnsiTypes.doublePrecision.nullable, _ => AnsiTypes.doublePrecision.nullable)
  override given sqlNumericOptDouble: SqlFractional[Option[Double]] =
    SqlFractional.defaultInstance(_ => AnsiTypes.doublePrecision.nullable, _ => AnsiTypes.doublePrecision.nullable)
  override given sqlNumericBigDecimal: SqlFractional[BigDecimal] = SqlFractional.defaultInstance(identity, identity)
  override given sqlNumericOptBigDecimal: SqlFractional[Option[BigDecimal]] =
    SqlFractional.defaultInstance(identity, identity)

  type DbMath <: SqlDbMath
  val DbMath: DbMath
  trait SqlDbMath:
    def pow[A: SqlNumeric](a: DbValue[A], b: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Pow, Seq(a.unsafeAsAnyDbVal, b.unsafeAsAnyDbVal), b.tpe).lift

    def sqrt[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Sqrt, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def abs[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Abs, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def ceil[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Ceiling, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def floor[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Floor, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def toDegrees[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Degrees, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def toRadians[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Radians, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def log[A: SqlFractional](a: DbValue[A], b: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Log, Seq(a.unsafeAsAnyDbVal, b.unsafeAsAnyDbVal), b.tpe).lift

    def ln[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Ln, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def log10[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Log10, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def log2[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Log2, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def exp[A: SqlFractional](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Exp, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def sign[A: SqlNumeric](a: DbValue[A]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Exp, Seq(a.unsafeAsAnyDbVal), a.tpe).lift

    def pi[A](tpe: CastType[A])(using NotGiven[A <:< Option[_]]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Pi, Nil, tpe.castTypeType).cast(tpe)

    def random[A](tpe: CastType[A])(using NotGiven[A <:< Option[_]]): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Random, Nil, tpe.castTypeType).cast(tpe)

  trait SqlDbValueBaseImpl[A] extends SqlDbValueBase[A] {

    @targetName("dbEquals")
    override def ===(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.BinOp(n.castDbVal(this.liftDbValue), n.castDbVal(rhs), n.wrapBinOp(SqlBinOp.Eq().liftSqlBinOp)).lift

    @targetName("dbNotEquals")
    override def !==(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.BinOp(n.castDbVal(this.liftDbValue), n.castDbVal(rhs), n.wrapBinOp(SqlBinOp.Neq().liftSqlBinOp)).lift

    @targetName("dbValCast") override def cast[B](tpe: CastType[B])(using n: Nullability[A]): DbValue[n.N[B]] =
      SqlDbValue.Cast(this.unsafeAsAnyDbVal, tpe.castTypeName, n.wrapType(tpe.castTypeType)).lift

    @targetName("dbValAsSome") override def asSome(using ev: NotGiven[A <:< Option[_]]): DbValue[Option[A]] =
      SqlDbValue.AsSome(this.liftDbValue, ev).lift

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
            .asInstanceOf[DbValue[Option[n.NNA]]]
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
            .asInstanceOf[DbValue[Option[n.NNA]]]
            .map(_ => DbValue.trueV)
            .asInstanceOf[DbValue[n.N[Boolean]]]
        else n.wrapDbVal(DbValue.trueV)
      else SqlDbValue.NotInValues(this.liftDbValue, values.map(_.as(tpe)), n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNullIf") override def nullIf(arg: DbValue[A]): DbValue[Nullable[A]] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.NullIf,
          Seq(this.unsafeAsAnyDbVal, arg.unsafeAsAnyDbVal),
          arg.tpe.typedChoice.nullable
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
      val argsList: List[AnyDbValue] = args.foldMapK([X] => (v: DbValue[X]) => List(v.unsafeAsAnyDbVal))
      val indicesState: State[Int, A[Const[Int]]] =
        args.traverseK([X] => (_: DbValue[X]) => State((acc: Int) => (acc + 1, acc)))
      val indices: A[Const[Int]] = indicesState.runA(0).value
      SqlDbValue.Custom(argsList, strArgs => render(indices.mapConst([X] => (i: Int) => strArgs(i))), tpe).lift

    override def functionK[A[_[_]]: TraverseKC, B](name: String, tpe: Type[B])(args: A[DbValue]): DbValue[B] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.Custom(name),
          args.foldMapK([X] => (v: DbValue[X]) => List(v.unsafeAsAnyDbVal)),
          tpe
        )
        .lift

    override def nullV[A](tpe: Type[A])(using ev: NotGiven[A <:< Option[_]]): DbValue[Option[A]] =
      SqlDbValue.Null(tpe, ev).lift

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

    case GetNullable(value: DbValue[Option[A]], ev: NotGiven[A <:< Option[_]])
    case AsSome[B](value: DbValue[B], ev: NotGiven[B <:< Option[_]]) extends SqlDbValue[Option[B]]

    case Placeholder(valueSeq: Seq[A], override val tpe: Type[A])
    case CompilePlaceholder(identifier: Object, override val tpe: Type[A])

    case SubSelect(query: Query[IdFC[A]])

    case Null[B](baseTpe: Type[B], ev: NotGiven[B <:< Option[_]]) extends SqlDbValue[Option[B]]
    case IsNull[B](value: DbValue[Option[B]])                     extends SqlDbValue[Boolean]
    case IsNotNull[B](value: DbValue[Option[B]])                  extends SqlDbValue[Boolean]

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

      case SqlDbValue.UnaryOp(value, op)  => value.ast.map(v => SqlExpr.UnaryOp(v, op.ast))
      case SqlDbValue.BinOp(lhs, rhs, op) => lhs.ast.flatMap(l => rhs.ast.map(r => SqlExpr.BinOp(l, r, op.ast)))

      case SqlDbValue.JoinNullable(value) => value.ast
      case SqlDbValue.Function(f, values, _) =>
        values.toList.traverse(_.ast).map(exprs => SqlExpr.FunctionCall(f, exprs))
      case SqlDbValue.Cast(value, typeName, _) => value.ast.map(v => SqlExpr.Cast(v, typeName))

      case SqlDbValue.GetNullable(value, _) => value.ast
      case SqlDbValue.AsSome(value, _)      => value.ast

      case SqlDbValue.Placeholder(value, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.SqlArgObj(value, tpe.codec)))

      case SqlDbValue.CompilePlaceholder(identifier, tpe) =>
        State.pure(SqlExpr.PreparedArgument(None, SqlArg.CompileArg(identifier, tpe.codec)))

        State.pure(SqlExpr.PreparedArgument(None, SqlArg.CompileArg(identifier, tpe.codec)))

      case SqlDbValue.SubSelect(query) => query.selectAstAndValues.map(m => SqlExpr.SubSelect(m.ast))

      case SqlDbValue.Null(_, _)       => State.pure(SqlExpr.Null())
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
      case SqlDbValue.QueryColumn(_, _, tpe) => tpe
      case SqlDbValue.UnaryOp(v, op)         => op.tpe(v)
      case SqlDbValue.BinOp(lhs, rhs, op)    => op.tpe(lhs, rhs)
      case SqlDbValue.JoinNullable(value) =>
        value.tpe.typedChoice.nullable.asInstanceOf[Type[A]]

      case SqlDbValue.Function(_, _, tpe) => tpe
      case SqlDbValue.Cast(_, _, tpe)     => tpe
      case v @ SqlDbValue.GetNullable(value, _) =>
        value.tpe.typedChoice.notNull
      case v @ SqlDbValue.AsSome(value, _) =>
        value.tpe.typedChoice.nullable
      case SqlDbValue.Placeholder(_, tpe)        => tpe
      case SqlDbValue.CompilePlaceholder(_, tpe) => tpe
      case SqlDbValue.SubSelect(query)           => query.selectAstAndValues.runA(freshTaggedState).value.values.tpe
      case v @ SqlDbValue.Null(tpe, _) =>
        tpe.typedChoice.nullable
      case SqlDbValue.IsNull(_)                => AnsiTypes.boolean.notNull
      case SqlDbValue.IsNotNull(_)             => AnsiTypes.boolean.notNull
      case SqlDbValue.InValues(_, _, tpe)      => tpe
      case SqlDbValue.NotInValues(_, _, tpe)   => tpe
      case SqlDbValue.InQuery(_, _, tpe)       => tpe
      case SqlDbValue.NotInQuery(_, _, tpe)    => tpe
      case SqlDbValue.ValueCase(_, _, orElse)  => orElse.tpe
      case SqlDbValue.ConditionCase(_, orElse) => orElse.tpe
      case SqlDbValue.Custom(_, _, tpe)        => tpe
      case SqlDbValue.QueryCount               => AnsiTypes.bigint.notNull
      case SqlDbValue.True                     => AnsiTypes.boolean.notNull
      case SqlDbValue.False                    => AnsiTypes.boolean.notNull
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
      case SqlDbValue.GetNullable(value, _)    => value.columnName(prefix)
      case SqlDbValue.AsSome(value, _)         => value.columnName(prefix)
      case SqlDbValue.Placeholder(_, _)        => prefix
      case SqlDbValue.CompilePlaceholder(_, _) => prefix
      case SqlDbValue.SubSelect(query) =>
        query.selectAstAndValues.runA(freshTaggedState).value.values.columnName(prefix)
      case SqlDbValue.Null(_, _)           => s"${prefix}_prefix"
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

    override def unsafeAsAnyDbVal: AnyDbValue = Impl.unsafeAsAnyDbVal(this.lift)

    override protected def liftDbValue: DbValue[A] = this.lift

    override def asc: Ord = Impl.asc(this.lift)

    override def desc: Ord = Impl.desc(this.lift)
  }

  given [A]: Lift[SqlDbValue[A], DbValue[A]] = sqlDbValueLift[A]
  protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]]

  extension [A](v: A)
    @targetName("valueAs") override def as(tpe: Type[A]): DbValue[A] = SqlDbValue.Placeholder(Seq(v), tpe).lift
    @targetName("valueAsNullable") override def asNullable(
        tpe: Type[A]
    )(using NotGiven[A <:< Option[_]]): DbValue[Option[A]] =
      (Some(v): Option[A]).as(tpe.typedChoice.nullable)

  trait SqlLogic[A] extends SqlLogicBase[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A] = rhs.tpe

    extension (lhs: DbValue[A])
      @targetName("and") def &&(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.And(this).liftSqlBinOp).lift

      @targetName("or") def ||(rhs: DbValue[A]): DbValue[A] =
        SqlDbValue.BinOp(lhs, rhs, SqlBinOp.Or(this).liftSqlBinOp).lift

      @targetName("not") def unary_! : DbValue[A] =
        SqlDbValue.UnaryOp(lhs, SqlUnaryOp.Not(this).liftSqlUnaryOp).lift
  object SqlLogic:
    def defaultInstance[A]: SqlLogic[A] = new SqlLogic[A] {}

  given booleanSqlLogic: SqlLogic[Boolean]            = SqlLogic.defaultInstance
  given booleanOptSqlLogic: SqlLogic[Option[Boolean]] = SqlLogic.defaultInstance

  // TODO: From this point onwards, move definitions to Base

  opaque type Many[A] = DbValue[Any] // Scala compiler bug? Stack overflow
  object Many {
    extension [A](many: Many[A])
      // TODO: Check that the return type is indeed Long on all platforms
      def count: DbValue[Long] =
        SqlDbValue
          .Function(SqlExpr.FunctionName.Count, Seq(many.unsafeAsDbValue.unsafeAsAnyDbVal), AnsiTypes.bigint.notNull)
          .lift

      inline def unsafeAsDbValue: DbValue[A] = many.asInstanceOf[DbValue[A]]

      def map[B](f: DbValue[A] => DbValue[B]): Many[B] = f(many.asInstanceOf[DbValue[A]]).asInstanceOf[DbValue[Any]]
  }
  extension [T](t: T)(using mr: MapRes[Many, T])
    def mapManyN[B](f: mr.K[DbValue] => DbValue[B]): Many[B] =
      f(mr.toK(t).asInstanceOf[mr.K[DbValue]]).asInstanceOf[Many[B]]

  extension [A](optVal: DbValue[Option[A]])(using ev: NotGiven[A <:< Option[_]])
    @targetName("dbValOptgetUnsafe") def unsafeGet: DbValue[A] =
      SqlDbValue.GetNullable(optVal, ev).lift

    @targetName("dbValOptMap") def map[B](f: DbValue[A] => DbValue[B]): DbValue[Option[B]] =
      Case.when(optVal.isDefined)(f(optVal.unsafeGet).asSome).otherwise(DbValue.nullV(f(optVal.unsafeGet).tpe))

    @targetName("dbValOptFilter") def filter(f: DbValue[A] => DbValue[Boolean]): DbValue[Option[A]] =
      Case.when(optVal.isDefined && f(optVal.unsafeGet))(optVal).otherwise(DbValue.nullV(optVal.unsafeGet.tpe))

    @targetName("dbValOptFlatMap") def flatMap[B](f: DbValue[A] => DbValue[Option[B]]): DbValue[Option[B]] =
      Case.when(optVal.isDefined)(f(optVal.unsafeGet)).otherwise(DbValue.nullV(f(optVal.unsafeGet).unsafeGet.tpe))

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

  extension [T](t: T)(using mr: MapRes[Compose2[DbValue, Option], T])
    def mapNullableN[B](f: mr.K[DbValue] => DbValue[B]): DbValue[Option[B]] =
      given ApplyKC[mr.K]    = mr.applyKC
      given FoldableKC[mr.K] = mr.traverseKC
      val res                = f(mr.toK(t).mapK([Z] => (v: DbValue[Option[Z]]) => v.unsafeGet))
      Case
        .when(mr.toK(t).foldLeftK(DbValue.trueV)(acc => [Z] => (v: DbValue[Option[Z]]) => acc && v.isDefined))(
          res.asSome
        )
        .otherwise(DbValue.nullV(res.tpe))

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

  type Api <: SqlDbValueApi & QueryApi

  trait SqlDbValueApi {
    export platform.{
      ConditionCase,
      NullabilityOf,
      SqlFractional,
      SqlIntegral,
      SqlLogic,
      SqlNumeric,
      SqlOrdered,
      ValueCase0,
      ValueCase1
    }

    inline def Nullability: platform.Nullability.type = platform.Nullability

    type AnyDbValue         = platform.AnyDbValue
    type BinOp[LHS, RHS, R] = platform.BinOp[LHS, RHS, R]
    type UnaryOp[V, R]      = platform.UnaryOp[V, R]
    type CastType[A]        = platform.CastType[A]
    type Codec[A]           = platform.Codec[A]
    type Type[A]            = platform.Type[A]

    inline def DbMath: platform.DbMath            = platform.DbMath
    inline def DbValue: platform.DbValueCompanion = platform.DbValue
    inline def Many: platform.Many.type           = platform.Many

    // Type inference seems worse with export, so we do this instead. Also not sure how name clashes will work with export

    inline def Case: platform.CaseCompanion = platform.Case

    extension [A](v: A)
      @targetName("valueAs") inline def as(tpe: Type[A]): DbValue[A] = SqlDbValue.Placeholder(Seq(v), tpe).lift
      @targetName("valueAsNullable") inline def asNullable(
          tpe: Type[A]
      )(using NotGiven[A <:< Option[_]]): DbValue[Option[A]] = platform.asNullable(v)(tpe)

    extension [T](t: T)(using mr: MapRes[Many, T])
      inline def mapManyN[B](f: mr.K[DbValue] => DbValue[B]): Many[B] = platform.mapManyN(t)(f)

    extension [A](optVal: DbValue[Option[A]])(using ev: NotGiven[A <:< Option[_]])
      @targetName("dbValOptgetUnsafe") inline def unsafeGet: DbValue[A] = platform.unsafeGet(optVal)

      @targetName("dbValOptMap") inline def map[B](f: DbValue[A] => DbValue[B]): DbValue[Option[B]] =
        platform.map(optVal)(f)

      @targetName("dbValOptFilter") inline def filter(f: DbValue[A] => DbValue[Boolean]): DbValue[Option[A]] =
        platform.filter(optVal)(f)

      @targetName("dbValOptFlatMap") inline def flatMap[B](f: DbValue[A] => DbValue[Option[B]]): DbValue[Option[B]] =
        platform.flatMap(optVal)(f)

      @targetName("dbValOptIsEmpty") inline def isEmpty: DbValue[Boolean]     = platform.isEmpty(optVal)
      @targetName("dbValOptIsDefined") inline def isDefined: DbValue[Boolean] = platform.isDefined(optVal)

      @targetName("dbValOptOrElse") inline def orElse(other: DbValue[Option[A]]): DbValue[Option[A]] =
        platform.orElse(optVal)(other)

      @targetName("dbValOptGetOrElse") inline def getOrElse(other: DbValue[A]): DbValue[A] =
        platform.getOrElse(optVal)(other)

    extension [T](t: T)(using mr: MapRes[Compose2[DbValue, Option], T])
      inline def mapNullableN[B](f: mr.K[DbValue] => DbValue[B]): DbValue[Option[B]] = platform.mapNullableN(t)(f)
  }
}
