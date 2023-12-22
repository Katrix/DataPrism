package dataprism.sharedast

import dataprism.sql.*

sealed trait SqlExpr[Type[_]]
//noinspection ScalaUnusedSymbol
object SqlExpr {
  case class QueryRef[Type[_]](query: String, column: String)                            extends SqlExpr[Type]
  case class UnaryOp[Type[_]](expr: SqlExpr[Type], op: UnaryOperation)                   extends SqlExpr[Type]
  case class BinOp[Type[_]](lhs: SqlExpr[Type], rhs: SqlExpr[Type], op: BinaryOperation) extends SqlExpr[Type]
  case class FunctionCall[Type[_]](functionCall: FunctionName, args: Seq[SqlExpr[Type]]) extends SqlExpr[Type]
  case class PreparedArgument[Type[_]](name: Option[String], arg: SqlArg[Type])          extends SqlExpr[Type]
  case class IsNull[Type[_]](expr: SqlExpr[Type])                                        extends SqlExpr[Type]
  case class Cast[Type[_]](expr: SqlExpr[Type], asType: String)                          extends SqlExpr[Type]

  case class SubSelect[Type[_]](selectAst: SelectAst[Type]) extends SqlExpr[Type]
  case class QueryCount[Type[_]]()                          extends SqlExpr[Type]

  case class Custom[Type[_]](args: Seq[SqlExpr[Type]], render: Seq[SqlStr[Type]] => SqlStr[Type]) extends SqlExpr[Type]

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

    case Greatest
    case Least

    case Abs
    case Avg
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

case class SelectAst[Type[_]](
    data: SelectAst.Data[Type],
    orderLimit: SelectAst.OrderLimit[Type]
)

//noinspection ScalaUnusedSymbol
object SelectAst {
  sealed trait Data[Type[_]]
  object Data {
    case class SelectFrom[Type[_]](
        distinct: Option[SelectAst.Distinct[Type]],
        selectExprs: Seq[SelectAst.ExprWithAlias[Type]],
        from: Option[SelectAst.From[Type]],
        where: Option[SqlExpr[Type]],
        groupBy: Option[SelectAst.GroupBy[Type]],
        having: Option[SqlExpr[Type]]
    ) extends Data[Type]

    sealed trait SetOperatorData[Type[_]] extends Data[Type] {
      def lhs: Data[Type]
      def rhs: Data[Type]
      def all: Boolean
    }

    case class Values[Type[_]](
        valueExprs: Seq[Seq[SqlExpr[Type]]],
        alias: Option[String],
        columnAliases: Option[Seq[String]]
    ) extends Data[Type]

    case class Union[Type[_]](lhs: Data[Type], rhs: Data[Type], all: Boolean)     extends SetOperatorData[Type]
    case class Intersect[Type[_]](lhs: Data[Type], rhs: Data[Type], all: Boolean) extends SetOperatorData[Type]
    case class Except[Type[_]](lhs: Data[Type], rhs: Data[Type], all: Boolean)    extends SetOperatorData[Type]
  }
  case class OrderLimit[Type[_]](
      orderBy: Option[SelectAst.OrderBy[Type]],
      limitOffset: Option[SelectAst.LimitOffset],
      locks: Option[SelectAst.Locks]
  ) {

    def isEmpty: Boolean = orderBy.isEmpty && limitOffset.isEmpty && locks.isEmpty
  }

  case class Distinct[Type[_]](on: Seq[SqlExpr[Type]])

  case class ExprWithAlias[Type[_]](expr: SqlExpr[Type], alias: Option[String])

  sealed trait From[Type[_]]
  object From {
    case class FromTable[Type[_]](table: String, alias: Option[String])                     extends From[Type]
    case class FromQuery[Type[_]](selectAst: SelectAst[Type], alias: String)                extends From[Type]
    case class FromMulti[Type[_]](fst: From[Type], snd: From[Type])                         extends From[Type]
    case class CrossJoin[Type[_]](lhs: From[Type], rhs: From[Type])                         extends From[Type]
    case class InnerJoin[Type[_]](lhs: From[Type], rhs: From[Type], on: SqlExpr[Type])      extends From[Type]
    case class LeftOuterJoin[Type[_]](lhs: From[Type], rhs: From[Type], on: SqlExpr[Type])  extends From[Type]
    case class RightOuterJoin[Type[_]](lhs: From[Type], rhs: From[Type], on: SqlExpr[Type]) extends From[Type]
    case class FullOuterJoin[Type[_]](lhs: From[Type], rhs: From[Type], on: SqlExpr[Type])  extends From[Type]
  }

  case class GroupBy[Type[_]](exprs: Seq[SqlExpr[Type]])
  case class OrderBy[Type[_]](exprs: Seq[OrderExpr[Type]])

  case class OrderExpr[Type[_]](expr: SqlExpr[Type], dir: OrderDir, nullsOrder: Option[NullsOrder])
  enum OrderDir:
    case Asc
    case Desc

  enum NullsOrder:
    case NullsFirst
    case NullsLast

  case class LimitOffset(limit: Option[Int], offset: Int, withTies: Boolean)

  case class Locks() // TODO
}
