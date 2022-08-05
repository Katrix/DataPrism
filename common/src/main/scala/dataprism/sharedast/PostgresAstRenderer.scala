package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class PostgresAstRenderer extends AstRenderer {

  override protected def renderBinaryOp(lhs: SqlExpr, rhs: SqlExpr, op: SqlExpr.BinaryOperation): SqlStr =
    op match
      case SqlExpr.BinaryOperation.Concat     => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} # ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)
}
