package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class H2AstRenderer extends AstRenderer {

  override protected def renderUnaryOp(expr: SqlExpr, op: SqlExpr.UnaryOperation): SqlStr =
    op match
      case SqlExpr.UnaryOperation.BitwiseNot => sql"BITNOT(${renderExpr(expr)}})"
      case _                                 => super.renderUnaryOp(expr, op)

  override protected def renderBinaryOp(lhs: SqlExpr, rhs: SqlExpr, op: SqlExpr.BinaryOperation): SqlStr =
    op match
      case SqlExpr.BinaryOperation.BitwiseAnd => sql"BITAND(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseOr  => sql"BITOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"BITXOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)
}
