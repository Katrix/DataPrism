package dataprism.sharedast

import java.sql.SQLException

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class H2AstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends AstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def renderFrom(from: SelectAst.From[Codec]): SqlStr[Codec] = from match
    case SelectAst.From.FromQuery(_, _, true) => throw new SQLException("H2 does not support lateral")
    case _                                    => super.renderFrom(from)

  override protected def renderUnaryOp(expr: SqlExpr[Codec], op: SqlExpr.UnaryOperation, tpe: String): SqlStr[Codec] =
    op match
      case SqlExpr.UnaryOperation.BitwiseNot => renderFunctionCall(SqlExpr.FunctionName.Custom("BITNOT"), Seq(expr), tpe)
      case _                                 => super.renderUnaryOp(expr, op, tpe)

  override protected def renderBinaryOp(
      lhs: SqlExpr[Codec],
      rhs: SqlExpr[Codec],
      op: SqlExpr.BinaryOperation,
      tpe: String
  ): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.Concat => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case SqlExpr.BinaryOperation.BitwiseAnd =>
        renderFunctionCall(SqlExpr.FunctionName.Custom("BITAND"), Seq(lhs, rhs), tpe)
      case SqlExpr.BinaryOperation.BitwiseOr => renderFunctionCall(SqlExpr.FunctionName.Custom("BITOR"), Seq(lhs, rhs), tpe)
      case SqlExpr.BinaryOperation.BitwiseXOr =>
        renderFunctionCall(SqlExpr.FunctionName.Custom("BITXOR"), Seq(lhs, rhs), tpe)
      case _ => super.renderBinaryOp(lhs, rhs, op, tpe)

  override protected def renderPreparedArgument(arg: SqlExpr.PreparedArgument[Codec]): SqlStr[Codec] =
    SqlStr(s"CAST(? AS ${arg.arg.codec.name})", Seq(arg.arg))
}
