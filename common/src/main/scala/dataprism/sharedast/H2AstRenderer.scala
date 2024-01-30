package dataprism.sharedast

import dataprism.sql.*

import java.sql.SQLException

//noinspection SqlNoDataSourceInspection
class H2AstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec]) extends AstRenderer[Codec](ansiTypes) {

  override protected def renderFrom(from: SelectAst.From[Codec]): SqlStr[Codec] = from match
    case SelectAst.From.FromQuery(_, _, true) => throw new SQLException("H2 does not support lateral")
    case _                                    => super.renderFrom(from)

  override protected def renderUnaryOp(expr: SqlExpr[Codec], op: SqlExpr.UnaryOperation): SqlStr[Codec] =
    op match
      case SqlExpr.UnaryOperation.BitwiseNot => sql"BITNOT(${renderExpr(expr)}})"
      case _                                 => super.renderUnaryOp(expr, op)

  override protected def renderBinaryOp(
      lhs: SqlExpr[Codec],
      rhs: SqlExpr[Codec],
      op: SqlExpr.BinaryOperation
  ): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.BitwiseAnd => sql"BITAND(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseOr  => sql"BITOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"BITXOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)
}
