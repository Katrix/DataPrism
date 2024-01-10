package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class MySqlAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec]) extends AstRenderer[Codec](ansiTypes) {

  override protected def renderBinaryOp(lhs: SqlExpr[Codec], rhs: SqlExpr[Codec], op: SqlExpr.BinaryOperation): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} ^ ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr[Codec] =
    spaceConcat(
      sql"LIMIT ${limitOffset.offset.asArg(ansiTypes.integer.notNull.codec)}",
      limitOffset.limit.fold(sql"")(l => sql", ${l.asArg(ansiTypes.integer.notNull.codec)}")
    )
}
