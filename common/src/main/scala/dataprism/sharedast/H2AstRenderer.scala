package dataprism.sharedast

import java.sql.SQLException

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class H2AstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String) extends AstRenderer[Codec](ansiTypes, getCodecTypeName) {

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
      case SqlExpr.BinaryOperation.Concat     => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseAnd => sql"BITAND(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseOr  => sql"BITOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"BITXOR(${renderExpr(lhs)}, ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderPreparedArgument(arg: SqlExpr.PreparedArgument[Codec]): SqlStr[Codec] =
    SqlStr(s"CAST(? AS ${arg.arg.codec.name})", Seq(arg.arg))
}
