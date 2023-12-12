package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class MySqlAstRenderer[Type[_]](ansiTypes: AnsiTypes[Type]) extends AstRenderer[Type](ansiTypes) {

  override protected def renderBinaryOp(lhs: SqlExpr[Type], rhs: SqlExpr[Type], op: SqlExpr.BinaryOperation): SqlStr[Type] =
    op match
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} ^ ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr[Type] =
    spaceConcat(
      sql"LIMIT ${limitOffset.offset.asArg(ansiTypes.integer)}",
      limitOffset.limit.fold(sql"")(l => sql", ${l.asArg(ansiTypes.integer)}")
    )
}
