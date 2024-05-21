package dataprism.platform.sql

import scala.annotation.targetName
import scala.util.NotGiven

import dataprism.platform.base.MapRes
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.SqlStr
import perspective.*

trait SqlQueryPlatformDbValueBase extends SqlQueryPlatformBase { platform =>

  trait SqlUnaryOpBase[V, R] {
    def name: String

    def ast: SqlExpr.UnaryOperation

    def tpe(v: DbValue[V]): Type[R]

    def nullable(using NotGiven[V <:< Option[_]], NotGiven[R <:< Option[_]]): UnaryOp[Option[V], Option[R]]
  }
  type UnaryOp[V, R] <: SqlUnaryOpBase[V, R]

  trait SqlBinOpBase[LHS, RHS, R] {
    def name: String

    def ast: SqlExpr.BinaryOperation

    def tpe(lhs: DbValue[LHS], rhs: DbValue[RHS]): Type[R]

    def nullable(
        using NotGiven[LHS <:< Option[_]],
        NotGiven[RHS <:< Option[_]],
        NotGiven[R <:< Option[_]]
    ): BinOp[Option[LHS], Option[RHS], Option[R]]
  }
  type BinOp[LHS, RHS, R] <: SqlBinOpBase[LHS, RHS, R]

  type NullabilityOf[A] <: Nullability[A] = A match {
    case Option[b] => Nullability.Aux[A, b, Option]
    case _         => Nullability.Aux[A, A, Id]
  }

  trait Nullability[A] extends NullabilityBase[A]:
    def wrapDbVal[B](dbVal: DbValue[B]): DbValue[N[B]]
    def wrapType[B](tpe: Type[B]): Type[N[B]]
    def wrapBinOp[LHS, RHS, R](binOp: BinOp[LHS, RHS, R])(
        using NotGiven[LHS <:< Option[_]],
        NotGiven[RHS <:< Option[_]],
        NotGiven[R <:< Option[_]]
    ): BinOp[N[LHS], N[RHS], N[R]]
    def wrapUnaryOp[V, R](
        unaryOp: UnaryOp[V, R]
    )(using NotGiven[V <:< Option[_]], NotGiven[R <:< Option[_]]): UnaryOp[N[V], N[R]]
    def castDbVal(dbVal: DbValue[A]): DbValue[N[A]] = dbVal.asInstanceOf[DbValue[N[A]]]

  object Nullability:
    type Aux[A, NNA0, N0[_]] = Nullability[A] { type N[B] = N0[B]; type NNA = NNA0 }

    given notNull[A](using NotGiven[A <:< Option[_]]): Nullability.Aux[A, A, Id] = new Nullability[A]:
      type N[B] = B
      type NNA  = A

      override def isNullable: Boolean = false

      override def wrapOption[B](n: B): Option[B]                 = Some(n)
      override def nullableToOption[B](n: Nullable[B]): Option[B] = n.asInstanceOf[Option[B]]

      override def wrapDbVal[B](dbVal: DbValue[B]): DbValue[B] = dbVal
      override def wrapType[B](tpe: Type[B]): Type[B]          = tpe.typedChoice.notNull
      override def wrapBinOp[LHS, RHS, R](binOp: BinOp[LHS, RHS, R])(
          using NotGiven[LHS <:< Option[_]],
          NotGiven[RHS <:< Option[_]],
          NotGiven[R <:< Option[_]]
      ): BinOp[LHS, RHS, R] = binOp
      override def wrapUnaryOp[V, R](
          unaryOp: UnaryOp[V, R]
      )(using NotGiven[V <:< Option[_]], NotGiven[R <:< Option[_]]): UnaryOp[V, R] = unaryOp

    given nullable[A, NN](using A <:< Option[NN]): Nullability.Aux[A, NN, Option] = new Nullability[A]:
      type N[B] = Option[B]
      type NNA  = NN

      override def isNullable: Boolean = true

      override def wrapOption[B](n: Option[B]): Option[B]                 = n
      override def nullableToOption[B](n: Nullable[Option[B]]): Option[B] = n

      override def wrapDbVal[B](dbVal: DbValue[B]): DbValue[Option[B]] = dbVal.asSome
      override def wrapType[B](tpe: Type[B]): Type[Option[B]]          = tpe.typedChoice.nullable
      override def wrapBinOp[LHS, RHS, R](binOp: BinOp[LHS, RHS, R])(
          using NotGiven[LHS <:< Option[_]],
          NotGiven[RHS <:< Option[_]],
          NotGiven[R <:< Option[_]]
      ): BinOp[Option[LHS], Option[RHS], Option[R]] = binOp.nullable
      override def wrapUnaryOp[V, R](
          unaryOp: UnaryOp[V, R]
      )(using NotGiven[V <:< Option[_]], NotGiven[R <:< Option[_]]): UnaryOp[Option[V], Option[R]] = unaryOp.nullable

  trait SqlOrderedBase[A](using val n: NullabilityOf[A]):
    def greatest(head: DbValue[A], tail: DbValue[A]*): DbValue[A]

    def least(head: DbValue[A], tail: DbValue[A]*): DbValue[A]

    extension (lhs: DbValue[A])
      @targetName("lessThan") def <(rhs: DbValue[A]): DbValue[n.N[Boolean]]
      @targetName("lessOrEqual") def <=(rhs: DbValue[A]): DbValue[n.N[Boolean]]
      @targetName("greaterOrEqual") def >=(rhs: DbValue[A]): DbValue[n.N[Boolean]]
      @targetName("greatherThan") def >(rhs: DbValue[A]): DbValue[n.N[Boolean]]

      @targetName("leastExtension") def least(rhss: DbValue[A]*): DbValue[A]       = this.least(lhs, rhss*)
      @targetName("greatestExtension") def greatest(rhss: DbValue[A]*): DbValue[A] = this.greatest(lhs, rhss*)

  type SqlOrdered[A] <: SqlOrderedBase[A]

  trait SqlNumericBase[A] extends SqlOrderedBase[A]:
    def tpe(lhs: DbValue[A], rhs: DbValue[A]): Type[A]

    extension (lhs: DbValue[A])
      @targetName("plus") def +(rhs: DbValue[A]): DbValue[A]
      @targetName("minus") def -(rhs: DbValue[A]): DbValue[A]
      @targetName("times") def *(rhs: DbValue[A]): DbValue[A]
      @targetName("divide") def /(rhs: DbValue[A]): DbValue[Nullable[A]]
      @targetName("remainder") def %(rhs: DbValue[A]): DbValue[A]
      @targetName("negation") def unary_- : DbValue[A]

  type SqlNumeric[A] <: SqlNumericBase[A] & SqlOrdered[A]

  trait SqlFractionalBase[A] extends SqlNumericBase[A]
  type SqlFractional[A] <: SqlNumeric[A] & SqlFractionalBase[A]

  trait SqlIntegralBase[A] extends SqlNumericBase[A]
  type SqlIntegral[A] <: SqlNumeric[A] & SqlIntegralBase[A]

  given sqlNumericShort: SqlIntegral[Short]
  given sqlNumericOptShort: SqlIntegral[Option[Short]]
  given sqlNumericInt: SqlIntegral[Int]
  given sqlNumericOptInt: SqlIntegral[Option[Int]]
  given sqlNumericLong: SqlIntegral[Long]
  given sqlNumericOptLong: SqlIntegral[Option[Long]]
  given sqlNumericFloat: SqlFractional[Float]
  given sqlNumericOptFloat: SqlFractional[Option[Float]]
  given sqlNumericDouble: SqlFractional[Double]
  given sqlNumericOptDouble: SqlFractional[Option[Double]]
  given sqlNumericBigDecimal: SqlFractional[BigDecimal]
  given sqlNumericOptBigDecimal: SqlFractional[Option[BigDecimal]]

  type CastType[A]
  extension [A](t: CastType[A])
    def castTypeName: String
    def castTypeType: Type[A]

  trait SqlDbValueBase[A] extends DbValueBase[A] {

    @targetName("dbValCast") def cast[B](tpe: CastType[B])(using n: Nullability[A]): DbValue[n.N[B]]

    @targetName("dbValAsSome") def asSome(using ev: NotGiven[A <:< Option[_]]): DbValue[Option[A]]

    @targetName("dbValInValues") def in(head: DbValue[A], tail: DbValue[A]*)(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]]

    @targetName("dbValNotInValues") def notIn(head: DbValue[A], tail: DbValue[A]*)(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]]

    @targetName("dbValInQuery") def in(query: Query[IdFC[A]])(using n: Nullability[A]): DbValue[n.N[Boolean]]

    @targetName("dbValNotInQuery") def notIn(query: Query[IdFC[A]])(using n: Nullability[A]): DbValue[n.N[Boolean]]

    @targetName("dbValInAsSeq") def inAs(values: Seq[A], tpe: Type[A])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]]

    @targetName("dbValNotInAsSeq") def notInAs(values: Seq[A], tpe: Type[A])(
        using n: Nullability[A]
    ): DbValue[n.N[Boolean]]

    @targetName("dbValNullIf") def nullIf(arg: DbValue[A]): DbValue[Nullable[A]]

    def ast: TagState[SqlExpr[Codec]]

    def tpe: Type[A]

    def columnName(prefix: String): String

    def unsafeAsAnyDbVal: AnyDbValue

    def unsafeDbValAsMany: Many[A]
  }

  override type DbValue[A] <: SqlDbValueBase[A]
  type AnyDbValue <: DbValue[Any]

  type DbValueCompanion <: SqlDbValueCompanion
  val DbValue: DbValueCompanion

  trait SqlDbValueCompanion {
    def rawK[A[_[_]]: TraverseKC, B](args: A[DbValue], tpe: Type[B])(
        render: A[Const[SqlStr[Codec]]] => SqlStr[Codec]
    ): DbValue[B]

    inline def raw[T, A](args: T, tpe: Type[A])(using mr: MapRes[DbValue, T])(
        render: mr.K[Const[SqlStr[Codec]]] => SqlStr[Codec]
    ): DbValue[A] = rawK(mr.toK(args), tpe)(render)(using mr.traverseKC)

    def functionK[A[_[_]]: TraverseKC, B](name: String, tpe: Type[B])(args: A[DbValue]): DbValue[B]

    inline def function[T, B](name: String, tpe: Type[B])(args: T)(using mr: MapRes[DbValue, T]): DbValue[B] =
      functionK(name, tpe)(mr.toK(args))(using mr.traverseKC)

    def nullV[A](tpe: Type[A])(using ev: NotGiven[A <:< Option[_]]): DbValue[Option[A]]

    def trueV: DbValue[Boolean]

    def falseV: DbValue[Boolean]
  }

  extension [A](v: A)
    @targetName("valueAs") def as(tpe: Type[A]): DbValue[A]
    @targetName("valueAsNullable") def asNullable(tpe: Type[A])(using NotGiven[A <:< Option[_]]): DbValue[Option[A]]

  trait SqlOrdSeqBase extends OrdSeqBase {
    def ast: TagState[Seq[SelectAst.OrderExpr[Codec]]]
  }
  type OrdSeq <: SqlOrdSeqBase

  trait SqlLogicBase[A]:
    extension (lhs: DbValue[A])
      @targetName("and") def &&(rhs: DbValue[A]): DbValue[A]

      @targetName("or") def ||(rhs: DbValue[A]): DbValue[A]

      @targetName("not") def unary_! : DbValue[A]

  type SqlLogic[A] <: SqlLogicBase[A]

}
