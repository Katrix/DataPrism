package dataprism.sharedast

import java.sql.SQLException

import cats.syntax.all.*
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class SqliteAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends AstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def renderBinaryOp(
      lhs: SqlExpr[Codec],
      rhs: SqlExpr[Codec],
      op: SqlExpr.BinaryOperation
  ): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.Concat => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case _                              => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderFunctionCall(call: SqlExpr.FunctionName, args: Seq[SqlExpr[Codec]]): SqlStr[Codec] =
    inline def rendered                         = args.map(renderExpr).intercalate(sql", ")
    inline def normal(f: String): SqlStr[Codec] = sql"${SqlStr.const(f)}($rendered)"
    call match
      case SqlExpr.FunctionName.Max      => normal("max")
      case SqlExpr.FunctionName.Greatest => normal("max")
      case SqlExpr.FunctionName.Min      => normal("min")
      case SqlExpr.FunctionName.Least    => normal("min")
      case _                             => super.renderFunctionCall(call, args)

  override protected def renderFrom(from: SelectAst.From[Codec]): SqlStr[Codec] = from match
    case SelectAst.From.FromQuery(_, _, true) => throw new SQLException("H2 does not support lateral")
    case _                                    => super.renderFrom(from)

  override protected def parenthesisAroundSetOps: Boolean = false

  override protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr[Codec] =
    spaceConcat(
      sql"LIMIT ${limitOffset.limit.getOrElse(-1).asArg(ansiTypes.integer.notNull.codec)}",
      sql"OFFSET ${limitOffset.offset.asArg(ansiTypes.integer.notNull.codec)}"
    )
}
