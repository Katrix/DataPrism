package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class H2AstRenderer[Type[_]](ansiTypes: AnsiTypes[Type]) extends AstRenderer[Type](ansiTypes) {

  override protected def renderUnaryOp(expr: SqlExpr[Type], op: SqlExpr.UnaryOperation): SqlStr[Type] =
    op match
      case SqlExpr.UnaryOperation.BitwiseNot => sql"BITNOT(${renderExpr(expr)}})"
      case _                                 => super.renderUnaryOp(expr, op)

  override protected def renderBinaryOp(lhs: SqlExpr[Type], rhs: SqlExpr[Type], op: SqlExpr.BinaryOperation): SqlStr[Type] =
    op match
      case SqlExpr.BinaryOperation.BitwiseAnd => sql"BITAND(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseOr  => sql"BITOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"BITXOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)
}
