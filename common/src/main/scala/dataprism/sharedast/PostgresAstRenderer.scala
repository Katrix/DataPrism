package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class PostgresAstRenderer[Type[_]](ansiTypes: AnsiTypes[Type]) extends AstRenderer[Type](ansiTypes) {

  override protected def renderBinaryOp(lhs: SqlExpr[Type], rhs: SqlExpr[Type], op: SqlExpr.BinaryOperation): SqlStr[Type] =
    op match
      case SqlExpr.BinaryOperation.Concat     => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} # ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)
}
