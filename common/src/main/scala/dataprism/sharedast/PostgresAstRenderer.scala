package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class PostgresAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec]) extends AstRenderer[Codec](ansiTypes) {

  override protected def renderBinaryOp(lhs: SqlExpr[Codec], rhs: SqlExpr[Codec], op: SqlExpr.BinaryOperation): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.Concat     => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} # ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)
}
