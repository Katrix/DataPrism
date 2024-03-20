package dataprism.sharedast

import cats.syntax.all.*
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
abstract class MySqlAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends AstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override def quote(s: String): String = s"`$s`"

  override def quoteSql(sql: SqlStr[Codec]): SqlStr[Codec] =
    if sql.args.nonEmpty then sql
    else
      val str = sql.str
      if str.startsWith("`") && str.endsWith("`") then sql
      else SqlStr.const(quote(str))

  override protected def renderBinaryOp(
      lhs: SqlExpr[Codec],
      rhs: SqlExpr[Codec],
      op: SqlExpr.BinaryOperation
  ): SqlStr[Codec] =
    op match
      case SqlExpr.BinaryOperation.BitwiseXOr => sql"(${renderExpr(lhs)} ^ ${renderExpr(rhs)})"
      case _                                  => super.renderBinaryOp(lhs, rhs, op)

  override protected def renderFunctionCall(call: SqlExpr.FunctionName, args: Seq[SqlExpr[Codec]]): SqlStr[Codec] =
    inline def rendered                         = args.map(renderExpr).intercalate(sql", ")
    inline def normal(f: String): SqlStr[Codec] = sql"${SqlStr.const(f)}($rendered)"

    call match
      case SqlExpr.FunctionName.Random                       => normal("rand")
      case SqlExpr.FunctionName.Greatest if args.length == 1 => renderExpr(args.head)
      case SqlExpr.FunctionName.Least if args.length == 1    => renderExpr(args.head)
      case _                                                 => super.renderFunctionCall(call, args)

  override protected def renderRow(row: Seq[SqlExpr[Codec]]): SqlStr[Codec] = sql"ROW${super.renderRow(row)}"

  override protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr[Codec] =
    // Some large number.....
    spaceConcat(
      sql"LIMIT ${limitOffset.limit.fold(SqlStr.const("18446744073709551615"))(_.asArg(ansiTypes.integer.notNull.codec))}",
      sql"OFFSET ${limitOffset.offset.asArg(ansiTypes.integer.notNull.codec)}"
    )

  override def renderDelete(query: SelectAst[Codec], returning: Boolean): SqlStr[Codec] =
    val (alias, from, where, exprs) = query match {
      case SelectAst.SelectFrom(
            None,
            exprs,
            Some(from @ SelectAst.From.CrossJoin(SelectAst.From.FromTable(table, alias), _)),
            where,
            None,
            None,
            None,
            None,
            None
          ) =>
        (alias.getOrElse(table), from, where, exprs)

      case SelectAst.SelectFrom(
            None,
            exprs,
            Some(from @ SelectAst.From.FromTable(table, alias)),
            where,
            None,
            None,
            None,
            None,
            None
          ) =>
        (alias.getOrElse(table), from, where, exprs)

      case _ =>
        // TODO: Enforce statically in the API
        throw new IllegalArgumentException("Can't use any other operator than from stuff and where with renderDelete")
    }

    spaceConcat(
      sql"DELETE FROM",
      SqlStr.const(quote(alias)),
      sql"USING",
      renderFrom(from),
      where.fold(sql"")(renderWhere),
      if returning then sql"RETURNING ${exprs.map(renderExprWithAlias).intercalate(sql", ")}" else sql""
    )
  end renderDelete
}
