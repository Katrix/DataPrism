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
      op: SqlExpr.BinaryOperation,
      tpe: String
  ): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.Concat => sql"(${renderExpr(lhs)} || ${renderExpr(rhs)})"
      case _                              => super.renderBinaryOp(lhs, rhs, op, tpe)

  override protected def renderFunctionCall(call: SqlExpr.FunctionName, args: Seq[SqlExpr[Codec]], tpe: String): SqlStr[Codec] =
    inline def rendered                         = args.map(renderExpr).intercalate(sql", ")
    inline def normal(f: String): SqlStr[Codec] = sql"${SqlStr.const(f)}($rendered)"

    def subdivided(f: String) =
      if args.length > 100 then
        val slidingArgs            = args.sliding(99).toSeq
        val (handled, notHandleds) = slidingArgs.splitAt(99)
        val handledArgs            = handled.map(args => SqlExpr.FunctionCall(call, args, tpe))
        val notHandledArg = if notHandleds.nonEmpty then Seq(SqlExpr.FunctionCall(call, notHandleds.flatten, tpe)) else Nil

        val allArgs = handledArgs ++ notHandledArg

        sql"${SqlStr.const(f)}(${allArgs.map(renderExpr).intercalate(sql", ")})"
      else normal(f)

    call match
      case SqlExpr.FunctionName.Max      => normal("max")
      case SqlExpr.FunctionName.Greatest => subdivided("max")
      case SqlExpr.FunctionName.Min      => normal("min")
      case SqlExpr.FunctionName.Least    => subdivided("min")
      case _                             => super.renderFunctionCall(call, args, tpe)

  override protected def renderFrom(from: SelectAst.From[Codec]): SqlStr[Codec] = from match
    case SelectAst.From.FromQuery(_, _, true) => throw new SQLException("H2 does not support lateral")
    case _                                    => super.renderFrom(from)

  override protected def parenthesisAroundSetOps: Boolean = false

  // Swap the order
  override protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset[Codec]): SqlStr[Codec] =
    spaceConcat(renderLimit(limitOffset).getOrElse(sql""), renderOffset(limitOffset).getOrElse(sql""))

  override protected def renderLimit(limitOffset: SelectAst.LimitOffset[Codec]): Option[SqlStr[Codec]] =
    Some(sql"LIMIT ${limitOffset.limit.fold(SqlStr.const("-1"))(renderExpr)}")
}
