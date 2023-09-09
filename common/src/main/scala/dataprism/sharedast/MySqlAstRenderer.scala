package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class MySqlAstRenderer extends AstRenderer {

  override protected def renderBinaryOp(lhs: SqlExpr, rhs: SqlExpr, op: SqlExpr.BinaryOperation): SqlStr =
    op match
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} ^ ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr =
    spaceConcat(
      sql"LIMIT ${limitOffset.offset.asArg(DbType.int4)}",
      limitOffset.limit.fold(sql"")(l => sql", ${l.asArg(DbType.int4)}")
    )
}
