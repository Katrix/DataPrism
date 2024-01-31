package dataprism.platform.sql

import java.sql.{Date, Time, Timestamp}

import scala.annotation.targetName
import scala.util.NotGiven

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlQueryPlatformDbValue { platform: SqlQueryPlatform =>

  trait SqlUnaryOpBase[V, R] {
    def name: String
    def ast: SqlExpr.UnaryOperation

    def tpe(v: DbValue[V]): Type[R]
  }

  type UnaryOp[V, R] <: SqlUnaryOpBase[V, R]

  trait SqlBinOpBase[LHS, RHS, R] {
    def name: String
    def ast: SqlExpr.BinaryOperation

    def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[R]
  }

  extension [A](tpe: Type[A]) @targetName("typeName") def name: String

  extension [A](tpe: Type[A])
    @targetName("typeTypedChoiceNotNull") protected def typedChoice(
        using NotGiven[A <:< Option[_]]
    ): NullabilityTypeChoice[Codec, A] = tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A]]

  extension [A](tpe: Type[Option[A]])
    @targetName("typeTypedChoiceNullable") protected def typedChoice: NullabilityTypeChoice[Codec, A] =
      tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A]]

  type BinOp[LHS, RHS, R] <: SqlBinOpBase[LHS, RHS, R]

  enum SqlUnaryOp[V, R](val name: String, op: SqlExpr.UnaryOperation) extends SqlUnaryOpBase[V, R] {
    case Not                                     extends SqlUnaryOp[Boolean, Boolean]("not", SqlExpr.UnaryOperation.Not)
    case Negative[A](numeric: SqlNumeric[A])     extends SqlUnaryOp[A, A]("negation", SqlExpr.UnaryOperation.Negation)
    case NullableOp[V1, R1](op: UnaryOp[V1, R1]) extends SqlUnaryOp[Option[V1], Option[R1]](op.name, op.ast)

    override def ast: SqlExpr.UnaryOperation = op

    override def tpe(v: DbValue[V]): Type[R] = this match
      case Not               => AnsiTypes.boolean.notNull
      case Negative(numeric) => numeric.tpe(v, v)
      case nop: NullableOp[v1, r1] =>
        given ev1: (DbValue[V] =:= DbValue[Option[v1]]) = <:<.refl
        given ev2: (Type[Option[r1]] =:= Type[R])       = <:<.refl

        ev2(nop.op.tpe(ev1(v).unsafeGet).typedChoice.nullable)
  }

  extension [V, R](op: SqlUnaryOp[V, R]) def liftSqlUnaryOp: UnaryOp[V, R]

  enum SqlBinOp[LHS, RHS, R](val name: String, op: SqlExpr.BinaryOperation) extends SqlBinOpBase[LHS, RHS, R] {
    case Eq[A]()             extends SqlBinOp[A, A, Boolean]("eq", SqlExpr.BinaryOperation.Eq)
    case Neq[A]()            extends SqlBinOp[A, A, Boolean]("neq", SqlExpr.BinaryOperation.Neq)
    case And                 extends SqlBinOp[Boolean, Boolean, Boolean]("and", SqlExpr.BinaryOperation.BoolAnd)
    case Or                  extends SqlBinOp[Boolean, Boolean, Boolean]("or", SqlExpr.BinaryOperation.BoolOr)
    case LessThan[A]()       extends SqlBinOp[A, A, Boolean]("lt", SqlExpr.BinaryOperation.LessThan)
    case LessOrEqual[A]()    extends SqlBinOp[A, A, Boolean]("le", SqlExpr.BinaryOperation.LessOrEq)
    case GreaterThan[A]()    extends SqlBinOp[A, A, Boolean]("gt", SqlExpr.BinaryOperation.GreaterThan)
    case GreaterOrEqual[A]() extends SqlBinOp[A, A, Boolean]("ge", SqlExpr.BinaryOperation.GreaterOrEq)

    case Plus[A](numeric: SqlNumeric[A])     extends SqlBinOp[A, A, A]("plus", SqlExpr.BinaryOperation.Plus)
    case Minus[A](numeric: SqlNumeric[A])    extends SqlBinOp[A, A, A]("minus", SqlExpr.BinaryOperation.Minus)
    case Multiply[A](numeric: SqlNumeric[A]) extends SqlBinOp[A, A, A]("times", SqlExpr.BinaryOperation.Multiply)
    case Divide[A](numeric: SqlNumeric[A])   extends SqlBinOp[A, A, A]("divide", SqlExpr.BinaryOperation.Divide)

    case NullableOp[LHS1, RHS1, R1](binop: BinOp[LHS1, RHS1, R1])
        extends SqlBinOp[Option[LHS1], Option[RHS1], Option[R1]](binop.name, binop.ast)

    override def ast: SqlExpr.BinaryOperation = op

    override def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[R] = this match
      case Eq()             => AnsiTypes.boolean.notNull
      case Neq()            => AnsiTypes.boolean.notNull
      case And              => AnsiTypes.boolean.notNull
      case Or               => AnsiTypes.boolean.notNull
      case LessThan()       => AnsiTypes.boolean.notNull
      case LessOrEqual()    => AnsiTypes.boolean.notNull
      case GreaterThan()    => AnsiTypes.boolean.notNull
      case GreaterOrEqual() => AnsiTypes.boolean.notNull

      case Plus(numeric)     => numeric.tpe(lhs, rhs)
      case Minus(numeric)    => numeric.tpe(lhs, rhs)
      case Multiply(numeric) => numeric.tpe(lhs, rhs)
      case Divide(numeric)   => numeric.tpe(lhs, rhs)

      case nop: NullableOp[lhs1, rhs1, r1] =>
        given ev1: (DbValue[LHS] =:= DbValue[Option[lhs1]]) = <:<.refl
        given ev2: (DbValue[RHS] =:= DbValue[Option[rhs1]]) = <:<.refl
        given ev3: (Type[Option[r1]] =:= Type[R])           = <:<.refl

        ev3(nop.binop.tpe(ev1(lhs).unsafeGet, ev2(rhs).unsafeGet).typedChoice.nullable)
    end tpe
  }

  extension [LHS, RHS, R](op: SqlBinOp[LHS, RHS, R]) def liftSqlBinOp: BinOp[LHS, RHS, R]

  trait Nullability[A] extends NullabilityBase[A]:
    def wrapDbVal[B](dbVal: DbValue[B]): DbValue[N[B]]
    def wrapType[B](tpe: Type[B]): Type[N[B]]
    def wrapBinOp[LHS, RHS, R](binOp: BinOp[LHS, RHS, R]): BinOp[N[LHS], N[RHS], N[R]]
    def wrapUnaryOp[V, R](unaryOp: UnaryOp[V, R]): UnaryOp[N[V], N[R]]
    def castDbVal(dbVal: DbValue[A]): DbValue[N[A]] = dbVal.asInstanceOf[DbValue[N[A]]]

  type NullabilityOf[A] <: Nullability[A] = A match {
    case Option[b] => Nullability.Aux[A, b, Option]
    case _         => Nullability.Aux[A, A, Id]
  }

  object Nullability:
    type Aux[A, NNA0, N0[_]] = Nullability[A] { type N[B] = N0[B]; type NNA = NNA0 }

    given notNull[A](using NotGiven[A <:< Option[_]]): Nullability.Aux[A, A, Id] = new Nullability[A]:
      type N[B] = B
      type NNA  = A

      override def wrapOption[B](n: B): Option[B]                 = Some(n)
      override def nullableToOption[B](n: Nullable[B]): Option[B] = n.asInstanceOf[Option[B]]

      override def wrapDbVal[B](dbVal: DbValue[B]): DbValue[B]                           = dbVal
      override def wrapType[B](tpe: Type[B]): Type[B]                                    = tpe.typedChoice.notNull
      override def wrapBinOp[LHS, RHS, R](binOp: BinOp[LHS, RHS, R]): BinOp[LHS, RHS, R] = binOp
      override def wrapUnaryOp[V, R](unaryOp: UnaryOp[V, R]): UnaryOp[V, R]              = unaryOp

    given nullable[A, NN](using A <:< Option[NN]): Nullability.Aux[A, NN, Option] = new Nullability[A]:
      type N[B] = Option[B]
      type NNA  = NN

      override def wrapOption[B](n: Option[B]): Option[B]                 = n
      override def nullableToOption[B](n: Nullable[Option[B]]): Option[B] = n

      override def wrapDbVal[B](dbVal: DbValue[B]): DbValue[Option[B]] = dbVal.asSome
      override def wrapType[B](tpe: Type[B]): Type[Option[B]]          = tpe.typedChoice.nullable
      override def wrapBinOp[LHS, RHS, R](binOp: BinOp[LHS, RHS, R]): BinOp[Option[LHS], Option[RHS], Option[R]] =
        SqlBinOp.NullableOp(binOp).liftSqlBinOp
      override def wrapUnaryOp[V, R](unaryOp: UnaryOp[V, R]): UnaryOp[Option[V], Option[R]] =
        SqlUnaryOp.NullableOp(unaryOp).liftSqlUnaryOp

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
      def avg: DbValue[Nullable[A]] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Avg,
            Seq(lhs.unsafeAsAnyDbVal),
            lhs.unsafeAsDbValue.tpe.typedChoice.nullable
          )
          .lift
          .asInstanceOf[DbValue[Nullable[A]]]
      def sum: DbValue[Nullable[A]] =
        import Many.unsafeAsDbValue
        SqlDbValue
          .Function(
            SqlExpr.FunctionName.Sum,
            Seq(lhs.unsafeAsAnyDbVal),
            lhs.unsafeAsDbValue.tpe.typedChoice.nullable
          )
          .lift
          .asInstanceOf[DbValue[Nullable[A]]]

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

  given sqlNumericShort: SqlNumeric[Short]                      = SqlNumeric.defaultInstance
  given sqlNumericOptShort: SqlNumeric[Option[Short]]           = SqlNumeric.defaultInstance
  given sqlNumericInt: SqlNumeric[Int]                          = SqlNumeric.defaultInstance
  given sqlNumericOptInt: SqlNumeric[Option[Int]]               = SqlNumeric.defaultInstance
  given sqlNumericLong: SqlNumeric[Long]                        = SqlNumeric.defaultInstance
  given sqlNumericOptLong: SqlNumeric[Option[Long]]             = SqlNumeric.defaultInstance
  given sqlNumericFloat: SqlNumeric[Float]                      = SqlNumeric.defaultInstance
  given sqlNumericOptFloat: SqlNumeric[Option[Float]]           = SqlNumeric.defaultInstance
  given sqlNumericDouble: SqlNumeric[Double]                    = SqlNumeric.defaultInstance
  given sqlNumericOptDouble: SqlNumeric[Option[Double]]         = SqlNumeric.defaultInstance
  given sqlNumericBigDecimal: SqlNumeric[BigDecimal]            = SqlNumeric.defaultInstance
  given sqlNumericOptBigDecimal: SqlNumeric[Option[BigDecimal]] = SqlNumeric.defaultInstance

  trait SqlOrdered[A](using val n: NullabilityOf[A]):
    def greatest(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Greatest, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    def least(head: DbValue[A], tail: DbValue[A]*): DbValue[A] =
      SqlDbValue.Function(SqlExpr.FunctionName.Least, (head +: tail).map(_.unsafeAsAnyDbVal), head.tpe).lift

    extension (lhs: DbValue[A])
      @targetName("lessThan") def <(rhs: DbValue[A]): DbValue[n.N[Boolean]]
      @targetName("lessOrEqual") def <=(rhs: DbValue[A]): DbValue[n.N[Boolean]]
      @targetName("greaterOrEqual") def >=(rhs: DbValue[A]): DbValue[n.N[Boolean]]
      @targetName("greatherThan") def >(rhs: DbValue[A]): DbValue[n.N[Boolean]]

      @targetName("leastExtension") def least(rhss: DbValue[A]*): DbValue[A]       = this.least(lhs, rhss*)
      @targetName("greatestExtension") def greatest(rhss: DbValue[A]*): DbValue[A] = this.greatest(lhs, rhss*)

    extension (lhs: Many[A])
      // TODO: Having these in here is quite broad. Might want to tighten this
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
    def defaultInstance[A](using na: NullabilityOf[A]): SqlOrdered[A] = new SqlOrdered[A]:
      extension (lhs: DbValue[A])
        @targetName("lessThan") def <(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
          n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessThan().liftSqlBinOp).lift)
        @targetName("lessOrEqual") def <=(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
          n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.LessOrEqual().liftSqlBinOp).lift)
        @targetName("greaterOrEqual") def >=(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
          n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterOrEqual().liftSqlBinOp).lift)
        @targetName("greatherThan") def >(rhs: DbValue[A]): DbValue[n.N[Boolean]] =
          n.wrapDbVal(SqlDbValue.BinOp(lhs, rhs, SqlBinOp.GreaterThan().liftSqlBinOp).lift)

  given sqlOrderedShort: SqlOrdered[Short]                    = SqlOrdered.defaultInstance
  given sqlOrderedOptShort: SqlOrdered[Option[Short]]         = SqlOrdered.defaultInstance
  given sqlOrderedInt: SqlOrdered[Int]                        = SqlOrdered.defaultInstance
  given sqlOrderedOptInt: SqlOrdered[Option[Int]]             = SqlOrdered.defaultInstance
  given sqlOrderedLong: SqlOrdered[Long]                      = SqlOrdered.defaultInstance
  given sqlOrderedOptLong: SqlOrdered[Option[Long]]           = SqlOrdered.defaultInstance
  given sqlOrderedFloat: SqlOrdered[Float]                    = SqlOrdered.defaultInstance
  given sqlOrderedOptFloat: SqlOrdered[Option[Float]]         = SqlOrdered.defaultInstance
  given sqlOrderedDouble: SqlOrdered[Double]                  = SqlOrdered.defaultInstance
  given sqlOrderedOptDouble: SqlOrdered[Option[Double]]       = SqlOrdered.defaultInstance
  given sqlOrderedString: SqlOrdered[String]                  = SqlOrdered.defaultInstance
  given sqlOrderedOptString: SqlOrdered[Option[String]]       = SqlOrdered.defaultInstance
  given sqlOrderedDate: SqlOrdered[Date]                      = SqlOrdered.defaultInstance
  given sqlOrderedOptDate: SqlOrdered[Option[Date]]           = SqlOrdered.defaultInstance
  given sqlOrderedTime: SqlOrdered[Time]                      = SqlOrdered.defaultInstance
  given sqlOrderedOptTime: SqlOrdered[Option[Time]]           = SqlOrdered.defaultInstance
  given sqlOrderedTimestamp: SqlOrdered[Timestamp]            = SqlOrdered.defaultInstance
  given sqlOrderedOptTimestamp: SqlOrdered[Option[Timestamp]] = SqlOrdered.defaultInstance
  given sqlOrderedBigInt: SqlOrdered[BigDecimal]              = SqlOrdered.defaultInstance
  given sqlOrderedOptBigInt: SqlOrdered[Option[BigDecimal]]   = SqlOrdered.defaultInstance

  type CastType[A]
  extension [A](t: CastType[A])
    def castTypeName: String
    def castTypeType: Type[A]

  trait SqlDbValueBase[A] extends DbValueBase[A] {

    @targetName("dbEquals")
    override def ===(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.BinOp(n.castDbVal(this.liftDbValue), n.castDbVal(rhs), n.wrapBinOp(SqlBinOp.Eq().liftSqlBinOp)).lift

    @targetName("dbNotEquals")
    override def !==(rhs: DbValue[A])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.BinOp(n.castDbVal(this.liftDbValue), n.castDbVal(rhs), n.wrapBinOp(SqlBinOp.Neq().liftSqlBinOp)).lift

    @targetName("dbValCast") def cast[B](tpe: CastType[B])(using n: Nullability[A]): DbValue[n.N[B]] =
      SqlDbValue.Cast(this.unsafeAsAnyDbVal, tpe.castTypeName, n.wrapType(tpe.castTypeType)).lift

    @targetName("dbValAsSome") def asSome(using ev: NotGiven[A <:< Option[_]]): DbValue[Option[A]] =
      SqlDbValue.AsSome(this.liftDbValue, ev).lift

    @targetName("dbValInValues") def in(head: DbValue[A], tail: DbValue[A]*)(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      SqlDbValue.InValues(this.liftDbValue, head +: tail, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNotInValues") def notIn(head: DbValue[A], tail: DbValue[A]*)(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      SqlDbValue.NotInValues(this.liftDbValue, head +: tail, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValInQuery") def in(query: Query[IdFC[A]])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.InQuery(this.liftDbValue, query, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNotInQuery") def notIn(query: Query[IdFC[A]])(using n: Nullability[A]): DbValue[n.N[Boolean]] =
      SqlDbValue.NotInQuery(this.liftDbValue, query, n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValInAsSeq") def inAs(values: Seq[A], tpe: Type[A])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      if values.isEmpty then n.wrapDbVal(DbValue.falseV)
      else SqlDbValue.InValues(this.liftDbValue, values.map(_.as(tpe)), n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNotInAsSeq") def notInAs(values: Seq[A], tpe: Type[A])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]] =
      if values.isEmpty then n.wrapDbVal(DbValue.trueV)
      else SqlDbValue.NotInValues(this.liftDbValue, values.map(_.as(tpe)), n.wrapType(AnsiTypes.boolean.notNull)).lift

    @targetName("dbValNullIf") def nullIf(arg: DbValue[A]): DbValue[Nullable[A]] =
      SqlDbValue
        .Function(
          SqlExpr.FunctionName.NullIf,
          Seq(this.unsafeAsAnyDbVal, arg.unsafeAsAnyDbVal),
          arg.tpe.typedChoice.nullable
        )
        .lift
        .asInstanceOf[DbValue[Nullable[A]]]

    def ast: TagState[SqlExpr[Codec]]

    def asSqlDbVal: Option[SqlDbValue[A]]

    def tpe: Type[A]

    def columnName(prefix: String): String

    def unsafeAsAnyDbVal: AnyDbValue
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  type DbValueCompanion <: SqlDbValueCompanion
  val DbValue: DbValueCompanion
  trait SqlDbValueCompanion {
    def rawK[A[_[_]]: TraverseKC, B](args: A[DbValue], tpe: Type[B])(
        render: A[Const[SqlStr[Codec]]] => SqlStr[Codec]
    ): DbValue[B] =
      val argsList: List[AnyDbValue] = args.foldMapK([X] => (v: DbValue[X]) => List(v.unsafeAsAnyDbVal))
      val indicesState: State[Int, A[Const[Int]]] =
        args.traverseK([X] => (_: DbValue[X]) => State((acc: Int) => (acc + 1, acc)))
      val indices: A[Const[Int]] = indicesState.runA(0).value
      SqlDbValue.Custom(argsList, strArgs => render(indices.mapConst([X] => (i: Int) => strArgs(i))), tpe).lift

    inline def raw[T, A](args: T, tpe: Type[A])(using mr: MapRes[DbValue, T])(
        render: mr.K[Const[SqlStr[Codec]]] => SqlStr[Codec]
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

    def nullV[A](tpe: Type[A])(using ev: NotGiven[A <:< Option[_]]): DbValue[Option[A]] =
      SqlDbValue.Null(tpe, ev).lift

    def trueV: DbValue[Boolean]  = SqlDbValue.True.lift
    def falseV: DbValue[Boolean] = SqlDbValue.False.lift
  }

  enum SqlDbValue[A] extends SqlDbValueBase[A] {
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
        import v.given
        value.tpe.typedChoice.notNull
      case v @ SqlDbValue.AsSome(value, _) =>
        import v.given
        value.tpe.typedChoice.nullable
      case SqlDbValue.Placeholder(_, tpe)        => tpe
      case SqlDbValue.CompilePlaceholder(_, tpe) => tpe
      case SqlDbValue.SubSelect(query)           => query.selectAstAndValues.runA(freshTaggedState).value.values.tpe
      case v @ SqlDbValue.Null(tpe, _) =>
        import v.given
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
      case SqlDbValue.QueryColumn(queryName, fromName, _) =>
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

    override def unsafeAsAnyDbVal: AnyDbValue = this.lift.unsafeAsAnyDbVal

    override def liftDbValue: DbValue[A] = this.lift

    override def asc: Ord = this.lift.asc

    override def desc: Ord = this.lift.asc
  }
  given [A]: Lift[SqlDbValue[A], DbValue[A]] = sqlDbValueLift[A]

  protected def sqlDbValueLift[A]: Lift[SqlDbValue[A], DbValue[A]]

  extension [A](dbValue: DbValue[A])
    @targetName("dbValueAsMany") protected inline def unsafeDbValAsMany: Many[A] = dbValue.asInstanceOf[Many[A]]

  extension [A](v: A)
    @targetName("valueAs") def as(tpe: Type[A]): DbValue[A] = SqlDbValue.Placeholder(Seq(v), tpe).lift
    @targetName("valueAsNullable") def asNullable(
        tpe: Type[A]
    )(using NotGiven[A <:< Option[_]]): DbValue[Option[A]] =
      (Some(v): Option[A]).as(tpe.typedChoice.nullable)

  extension (boolVal: DbValue[Boolean])
    @targetName("dbValBooleanAnd") def &&(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.And.liftSqlBinOp).lift

    @targetName("dbValBooleanOr") def ||(that: DbValue[Boolean]): DbValue[Boolean] =
      SqlDbValue.BinOp(boolVal, that, SqlBinOp.Or.liftSqlBinOp).lift

    @targetName("dbValBooleanNot") def unary_! : DbValue[Boolean] =
      SqlDbValue.UnaryOp(boolVal, SqlUnaryOp.Not.liftSqlUnaryOp).lift

  extension (boolVal: DbValue[Option[Boolean]])
    @targetName("dbValNullableBooleanAnd") def &&(that: DbValue[Option[Boolean]]): DbValue[Option[Boolean]] =
      SqlDbValue
        .BinOp(
          boolVal,
          that,
          SqlBinOp.NullableOp(SqlBinOp.And.liftSqlBinOp).liftSqlBinOp
        )
        .lift

    @targetName("dbValNullableBooleanOr") def ||(that: DbValue[Option[Boolean]]): DbValue[Option[Boolean]] =
      SqlDbValue
        .BinOp(
          boolVal,
          that,
          SqlBinOp.NullableOp(SqlBinOp.Or.liftSqlBinOp).liftSqlBinOp
        )
        .lift

    @targetName("dbValNullableBooleanNot") def unary_! : DbValue[Option[Boolean]] =
      SqlDbValue
        .UnaryOp(
          boolVal,
          SqlUnaryOp.NullableOp(SqlUnaryOp.Not.liftSqlUnaryOp).liftSqlUnaryOp
        )
        .lift

  trait SqlOrdSeqBase extends OrdSeqBase {
    def ast: TagState[Seq[SelectAst.OrderExpr[Codec]]]
  }

  type OrdSeq <: SqlOrdSeqBase

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

  type Api <: SqlDbValueApi

  trait SqlDbValueApi {
    export platform.{ConditionCase, NullabilityOf, SqlNumeric, SqlOrdered, ValueCase0, ValueCase1}

    type AnyDbValue         = platform.AnyDbValue
    type BinOp[LHS, RHS, R] = platform.BinOp[LHS, RHS, R]
    type UnaryOp[V, R]      = platform.UnaryOp[V, R]
    type CastType[A]        = platform.CastType[A]

    inline def DbValue: platform.DbValueCompanion = platform.DbValue
    inline def Many: platform.Many.type           = platform.Many

    // Type inference seems worse with export, so we do this instead. Also not sure how name clashes will work with export

    inline def Case: platform.CaseCompanion = platform.Case

    extension [A](v: A)
      @targetName("valueAs") inline def as(tpe: Type[A]): DbValue[A] = SqlDbValue.Placeholder(Seq(v), tpe).lift
      @targetName("valueAsNullable") inline def asNullable(
          tpe: Type[A]
      )(using NotGiven[A <:< Option[_]]): DbValue[Option[A]] = platform.asNullable(v)(tpe)

    extension (boolVal: DbValue[Boolean])
      @targetName("dbValBooleanAnd") inline def &&(that: DbValue[Boolean]): DbValue[Boolean] =
        platform.&&(boolVal)(that)

      @targetName("dbValBooleanOr") inline def ||(that: DbValue[Boolean]): DbValue[Boolean] = platform.||(boolVal)(that)

      @targetName("dbValBooleanNot") inline def unary_! : DbValue[Boolean] = platform.unary_!(boolVal)

    extension (boolVal: DbValue[Option[Boolean]])
      @targetName("dbValNullableBooleanAnd") inline def &&(that: DbValue[Option[Boolean]]): DbValue[Option[Boolean]] =
        platform.&&(boolVal)(that)

      @targetName("dbValNullableBooleanOr") inline def ||(that: DbValue[Option[Boolean]]): DbValue[Option[Boolean]] =
        platform.||(boolVal)(that)

      @targetName("dbValNullableBooleanNot") inline def unary_! : DbValue[Option[Boolean]] = platform.unary_!(boolVal)

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
