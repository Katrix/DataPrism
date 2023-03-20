package dataprism.sharedast

import dataprism.sql.{SqlArg, SqlStr}

sealed trait SqlExpr
//noinspection ScalaUnusedSymbol
object SqlExpr {
  case class QueryRef(query: String, column: String)                      extends SqlExpr
  case class UnaryOp(expr: SqlExpr, op: UnaryOperation)                   extends SqlExpr
  case class BinOp(lhs: SqlExpr, rhs: SqlExpr, op: BinaryOperation)       extends SqlExpr
  case class FunctionCall(functionCall: FunctionName, args: Seq[SqlExpr]) extends SqlExpr
  case class PreparedArgument(name: Option[String], arg: SqlArg)          extends SqlExpr
  case class IsNull(expr: SqlExpr)                                        extends SqlExpr
  case class Cast(expr: SqlExpr, asType: String)                          extends SqlExpr

  case class SubSelect(selectAst: SelectAst) extends SqlExpr

  case class Custom(args: Seq[SqlExpr], render: Seq[SqlStr] => SqlStr) extends SqlExpr

  enum UnaryOperation:
    case Not
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
    case Modulo

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

    case Abs
    case Avg
    case Max
    case Min
    case Count
    case Sum

    case Ln
    case Log
    case Log10
    case Log2
    case Pow
    case Exp

    case Ceiling
    case Floor

    case Concat

    case Custom(f: String)
}

case class SelectAst(
    data: SelectAst.Data,
    orderLimit: SelectAst.OrderLimit
)

//noinspection ScalaUnusedSymbol
object SelectAst {
  sealed trait Data
  object Data {
    case class SelectFrom(
        distinct: Option[SelectAst.Distinct],
        selectExprs: Seq[SelectAst.ExprWithAlias],
        from: Option[SelectAst.From],
        where: Option[SqlExpr],
        groupBy: Option[SelectAst.GroupBy],
        having: Option[SqlExpr]
    ) extends Data

    sealed trait SetOperatorData extends Data {
      def lhs: Data
      def rhs: Data
      def all: Boolean
    }

    case class Union(lhs: Data, rhs: Data, all: Boolean)     extends SetOperatorData
    case class Intersect(lhs: Data, rhs: Data, all: Boolean) extends SetOperatorData
    case class Except(lhs: Data, rhs: Data, all: Boolean)    extends SetOperatorData
  }
  case class OrderLimit(
      orderBy: Option[SelectAst.OrderBy],
      limitOffset: Option[SelectAst.LimitOffset],
      locks: Option[SelectAst.Locks]
  ) {

    def isEmpty: Boolean = orderBy.isEmpty && limitOffset.isEmpty && locks.isEmpty
  }

  case class Distinct(on: Seq[SqlExpr])

  case class ExprWithAlias(expr: SqlExpr, alias: Option[String])

  sealed trait From
  object From {
    case class FromTable(table: String, alias: Option[String]) extends From
    case class FromQuery(selectAst: SelectAst, alias: String)  extends From
    case class FromValues(valueExprs: Seq[Seq[SqlExpr]], alias: Option[String], columnAliases: Option[Seq[String]])
        extends From
    case class FromMulti(fst: From, snd: From)                   extends From
    case class CrossJoin(lhs: From, rhs: From)                   extends From
    case class InnerJoin(lhs: From, rhs: From, on: SqlExpr)      extends From
    case class LeftOuterJoin(lhs: From, rhs: From, on: SqlExpr)  extends From
    case class RightOuterJoin(lhs: From, rhs: From, on: SqlExpr) extends From
    case class FullOuterJoin(lhs: From, rhs: From, on: SqlExpr)  extends From
  }

  case class GroupBy(exprs: Seq[SqlExpr])
  case class OrderBy(exprs: Seq[OrderExpr])

  case class OrderExpr(expr: SqlExpr, dir: OrderDir, nullsOrder: Option[NullsOrder])
  enum OrderDir:
    case Asc
    case Desc

  enum NullsOrder:
    case NullsFirst
    case NullsLast

  case class LimitOffset(limit: Option[Int], offset: Int, withTies: Boolean)

  case class Locks() // TODO
}
