package dataprism.sharedast

import cats.syntax.all.*
import dataprism.sql.*

sealed trait SqlExpr[Codec[_]]
//noinspection ScalaUnusedSymbol
object SqlExpr {
  case class QueryRef[Codec[_]](query: String, column: String)                                 extends SqlExpr[Codec]
  case class UnaryOp[Codec[_]](expr: SqlExpr[Codec], op: UnaryOperation, expectedType: String) extends SqlExpr[Codec]
  case class BinOp[Codec[_]](lhs: SqlExpr[Codec], rhs: SqlExpr[Codec], op: BinaryOperation, expectedType: String)
      extends SqlExpr[Codec]
  case class FunctionCall[Codec[_]](functionCall: FunctionName, args: Seq[SqlExpr[Codec]], expectedType: String)
      extends SqlExpr[Codec]
  case class PreparedArgument[Codec[_]](name: Option[String], arg: SqlArg[Codec])     extends SqlExpr[Codec]
  case class Null[Codec[_]]()                                                         extends SqlExpr[Codec]
  case class IsNull[Codec[_]](expr: SqlExpr[Codec])                                   extends SqlExpr[Codec]
  case class IsNotNull[Codec[_]](expr: SqlExpr[Codec])                                extends SqlExpr[Codec]
  case class InValues[Codec[_]](expr: SqlExpr[Codec], values: Seq[SqlExpr[Codec]])    extends SqlExpr[Codec]
  case class NotInValues[Codec[_]](expr: SqlExpr[Codec], values: Seq[SqlExpr[Codec]]) extends SqlExpr[Codec]
  case class InQuery[Codec[_]](expr: SqlExpr[Codec], selectAst: SelectAst[Codec])     extends SqlExpr[Codec]
  case class NotInQuery[Codec[_]](expr: SqlExpr[Codec], selectAst: SelectAst[Codec])  extends SqlExpr[Codec]
  case class Cast[Codec[_]](expr: SqlExpr[Codec], asType: String)                     extends SqlExpr[Codec]

  case class ValueCase[Codec[_]](
      matchOn: SqlExpr[Codec],
      cases: IndexedSeq[(SqlExpr[Codec], SqlExpr[Codec])],
      orElse: SqlExpr[Codec]
  ) extends SqlExpr[Codec]
  case class ConditionCase[Codec[_]](cases: IndexedSeq[(SqlExpr[Codec], SqlExpr[Codec])], orElse: SqlExpr[Codec])
      extends SqlExpr[Codec]

  case class SubSelect[Codec[_]](selectAst: SelectAst[Codec]) extends SqlExpr[Codec]
  case class QueryCount[Codec[_]]()                           extends SqlExpr[Codec]
  case class True[Codec[_]]()                                 extends SqlExpr[Codec]
  case class False[Codec[_]]()                                extends SqlExpr[Codec]

  case class Custom[Codec[_]](args: Seq[SqlExpr[Codec]], render: Seq[SqlStr[Codec]] => SqlStr[Codec])
      extends SqlExpr[Codec]

  enum UnaryOperation:
    case Not
    case Negation
    case BitwiseNot

  enum BinaryOperation:
    case Eq
    case Neq
    case GreaterThan
    case GreaterOrEq
    case LessThan
    case LessOrEq

    case BoolAnd
    case BoolOr

    case Concat

    case Plus
    case Minus
    case Multiply
    case Divide
    case Remainder

    case BitwiseAnd
    case BitwiseOr
    case BitwiseXOr
    case RightShift
    case LeftShift

    case Custom(op: String)

  enum FunctionName:
    case ACos
    case ASin
    case ATan
    case ATan2
    case Cos
    case Cot
    case Sin
    case Tan

    case Greatest
    case Least

    case Abs
    case Avg
    case Count
    case Sum
    case Min
    case Max

    case Ln
    case Log
    case Log10
    case Log2
    case Sqrt
    case Pow
    case Exp

    case Ceiling
    case Floor

    case Radians
    case Degrees

    case Sign
    case Pi
    case Random

    case Concat

    case Coalesce
    case NullIf

    case Custom(f: String)

    def name: String = this match
      case FunctionName.Custom(s) => s
      case _                      => this.toString
}

sealed trait SelectAst[Codec[_]]
//noinspection ScalaUnusedSymbol
object SelectAst {
  case class SelectFrom[Codec[_]](
      distinct: Option[SelectAst.Distinct[Codec]],
      selectExprs: Seq[SelectAst.ExprWithAlias[Codec]],
      from: Option[SelectAst.From[Codec]],
      where: Option[SqlExpr[Codec]],
      groupBy: Option[SelectAst.GroupBy[Codec]],
      having: Option[SqlExpr[Codec]],
      orderBy: Option[SelectAst.OrderBy[Codec]],
      limitOffset: Option[SelectAst.LimitOffset[Codec]],
      locks: Option[SelectAst.Locks]
  ) extends SelectAst[Codec]

  sealed trait SetOperator[Codec[_]] extends SelectAst[Codec] {
    def lhs: SelectAst[Codec]
    def rhs: SelectAst[Codec]
    def all: Boolean
  }

  case class Values[Codec[_]](
      valueExprs: Seq[Seq[SqlExpr[Codec]]],
      alias: Option[String],
      columnAliases: Option[Seq[String]]
  ) extends SelectAst[Codec]

  case class Union[Codec[_]](lhs: SelectAst[Codec], rhs: SelectAst[Codec], all: Boolean)     extends SetOperator[Codec]
  case class Intersect[Codec[_]](lhs: SelectAst[Codec], rhs: SelectAst[Codec], all: Boolean) extends SetOperator[Codec]
  case class Except[Codec[_]](lhs: SelectAst[Codec], rhs: SelectAst[Codec], all: Boolean)    extends SetOperator[Codec]

  case class Distinct[Codec[_]](on: Seq[SqlExpr[Codec]])

  case class ExprWithAlias[Codec[_]](expr: SqlExpr[Codec], alias: Option[String])

  sealed trait From[Codec[_]]
  object From {
    case class FromTable[Codec[_]](table: String, alias: Option[String])                         extends From[Codec]
    case class FromQuery[Codec[_]](selectAst: SelectAst[Codec], alias: String, lateral: Boolean) extends From[Codec]
    case class FromMulti[Codec[_]](fst: From[Codec], snd: From[Codec])                           extends From[Codec]
    case class CrossJoin[Codec[_]](lhs: From[Codec], rhs: From[Codec])                           extends From[Codec]
    case class InnerJoin[Codec[_]](lhs: From[Codec], rhs: From[Codec], on: SqlExpr[Codec])       extends From[Codec]
    case class LeftOuterJoin[Codec[_]](lhs: From[Codec], rhs: From[Codec], on: SqlExpr[Codec])   extends From[Codec]
    case class RightOuterJoin[Codec[_]](lhs: From[Codec], rhs: From[Codec], on: SqlExpr[Codec])  extends From[Codec]
    case class FullOuterJoin[Codec[_]](lhs: From[Codec], rhs: From[Codec], on: SqlExpr[Codec])   extends From[Codec]
  }

  case class GroupBy[Codec[_]](exprs: Seq[SqlExpr[Codec]])
  case class OrderBy[Codec[_]](exprs: Seq[OrderExpr[Codec]])

  case class OrderExpr[Codec[_]](expr: SqlExpr[Codec], dir: OrderDir, nullsOrder: Option[NullsOrder])
  enum OrderDir:
    case Asc
    case Desc

  enum NullsOrder:
    case NullsFirst
    case NullsLast

  case class LimitOffset[Codec[_]](limit: Option[SqlExpr[Codec]], offset: Option[SqlExpr[Codec]], withTies: Boolean)

  case class Locks() // TODO
}
