package dataprism.sharedast

import cats.syntax.all.*
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class AstRenderer {

  protected def renderUnaryOp(expr: SqlExpr, op: SqlExpr.UnaryOperation): SqlStr =
    val rendered = renderExpr(expr)
    op match
      case SqlExpr.UnaryOperation.Not        => sql"(NOT $rendered)"
      case SqlExpr.UnaryOperation.BitwiseNot => sql"(~$rendered)"

  protected def renderBinaryOp(lhs: SqlExpr, rhs: SqlExpr, op: SqlExpr.BinaryOperation): SqlStr =
    val lhsr                              = renderExpr(lhs)
    val rhsr                              = renderExpr(rhs)
    inline def normal(op: String): SqlStr = sql"($lhsr ${SqlStr.const(op)} $rhsr)"

    op match
      case SqlExpr.BinaryOperation.Eq          => normal("=")
      case SqlExpr.BinaryOperation.Neq         => normal("<>")
      case SqlExpr.BinaryOperation.GreaterThan => normal(">")
      case SqlExpr.BinaryOperation.GreaterOrEq => normal(">=")
      case SqlExpr.BinaryOperation.LessThan    => normal("<")
      case SqlExpr.BinaryOperation.LessOrEq    => normal("<=")

      case SqlExpr.BinaryOperation.BoolAnd => normal("AND")
      case SqlExpr.BinaryOperation.BoolOr  => normal("OR")

      case SqlExpr.BinaryOperation.Concat => sql"concat($lhsr, $rhsr)"

      case SqlExpr.BinaryOperation.Plus     => normal("+")
      case SqlExpr.BinaryOperation.Minus    => normal("-")
      case SqlExpr.BinaryOperation.Multiply => normal("*")
      case SqlExpr.BinaryOperation.Divide   => normal("/")
      case SqlExpr.BinaryOperation.Modulo   => normal("%")

      case SqlExpr.BinaryOperation.BitwiseAnd => normal("&")
      case SqlExpr.BinaryOperation.BitwiseOr  => normal("|")
      case SqlExpr.BinaryOperation.BitwiseXOr =>
        // ((~(lhs&rhs))&(lhs|rhs))

        // https://stackoverflow.com/a/16443025
        renderExpr(
          SqlExpr.BinOp(
            SqlExpr
              .UnaryOp(SqlExpr.BinOp(lhs, rhs, SqlExpr.BinaryOperation.BitwiseAnd), SqlExpr.UnaryOperation.BitwiseNot),
            SqlExpr.BinOp(lhs, rhs, SqlExpr.BinaryOperation.BitwiseOr),
            SqlExpr.BinaryOperation.BitwiseAnd
          )
        )
      case SqlExpr.BinaryOperation.RightShift => normal(">>")
      case SqlExpr.BinaryOperation.LeftShift  => normal("<<")

      case SqlExpr.BinaryOperation.Custom(op) => normal(op)

  protected def renderFunctionCall(call: SqlExpr.FunctionName, args: Seq[SqlExpr]): SqlStr =
    val rendered                         = args.map(renderExpr).intercalate(sql", ")
    inline def normal(f: String): SqlStr = sql"${SqlStr.const(f)}($rendered)"

    call match
      case SqlExpr.FunctionName.ACos  => normal("acos")
      case SqlExpr.FunctionName.ASin  => normal("asin")
      case SqlExpr.FunctionName.ATan  => normal("atan")
      case SqlExpr.FunctionName.ATan2 => normal("atan2")
      case SqlExpr.FunctionName.Cos   => normal("cos")
      case SqlExpr.FunctionName.Cot   => normal("cot")
      case SqlExpr.FunctionName.Sin   => normal("sin")
      case SqlExpr.FunctionName.Tan   => normal("tan")

      case SqlExpr.FunctionName.Abs => normal("abs")
      case SqlExpr.FunctionName.Avg => normal("avg")
      case SqlExpr.FunctionName.Max => normal("max")
      case SqlExpr.FunctionName.Min => normal("min")

      case SqlExpr.FunctionName.Ln      => normal("ln")
      case SqlExpr.FunctionName.Log     => normal("log")
      case SqlExpr.FunctionName.Log10   => normal("log10")
      case SqlExpr.FunctionName.Log2    => sql"log(2, $rendered)"
      case SqlExpr.FunctionName.Pow     => normal("power")
      case SqlExpr.FunctionName.Exp     => normal("exp")
      case SqlExpr.FunctionName.Ceiling => normal("ceil")
      case SqlExpr.FunctionName.Floor   => normal("floor")
      case SqlExpr.FunctionName.Concat  => normal("concat")

      case SqlExpr.FunctionName.Custom(f) => normal(f)

  protected def renderExpr(expr: SqlExpr): SqlStr = expr match
    case SqlExpr.QueryRef(query, column)          => SqlStr.const(s"$query.$column")
    case SqlExpr.UnaryOp(expr, op)                => renderUnaryOp(expr, op)
    case SqlExpr.BinOp(lhs, rhs, op)              => renderBinaryOp(lhs, rhs, op)
    case SqlExpr.FunctionCall(functionCall, args) => renderFunctionCall(functionCall, args)
    case SqlExpr.PreparedArgument(_, arg)         => SqlStr("?", Seq(arg))
    case SqlExpr.IsNull(expr)                     => sql"${renderExpr(expr)} IS NULL"
    case SqlExpr.Cast(expr, asType)               => sql"(CAST (${renderExpr(expr)} AS ${SqlStr.const(asType)}))"
    case SqlExpr.Custom(args, render)             => render(args.map(renderExpr))

  protected def spaceConcat(args: SqlStr*): SqlStr =
    args.filter(_.nonEmpty).intercalate(sql" ")

  def renderSelect(selectAst: SelectAst): SqlStr =
    spaceConcat(renderSelectData(selectAst.data), renderSelectOrderLimit(selectAst.orderLimit))

  protected def renderSelectData(data: SelectAst.Data): SqlStr = data match
    case d: SelectAst.Data.SelectFrom      => renderSelectFrom(d)
    case d: SelectAst.Data.SetOperatorData => renderSetOperatorData(d)

  protected def renderSelectFrom(data: SelectAst.Data.SelectFrom): SqlStr =
    val distinct = data.distinct.fold(sql"")(renderDistinct)
    val exprs    = data.selectExprs.map(renderExprWithAlias).intercalate(sql", ")
    val from     = data.from.fold(sql"")(f => sql"FROM ${renderFrom(f)}")
    val where    = data.where.fold(sql"")(renderWhere)
    val groupBy  = data.groupBy.fold(sql"")(renderGroupBy)
    val having   = data.having.fold(sql"")(renderHaving)
    spaceConcat(sql"SELECT", distinct, exprs, from, where, groupBy, having)

  protected def renderDistinct(distinct: SelectAst.Distinct): SqlStr =
    spaceConcat(
      sql"DISTINCT",
      if distinct.on.isEmpty then sql"" else sql"ON",
      distinct.on.map(renderExpr).intercalate(sql", ")
    )

  protected def renderExprWithAlias(exprWithAlias: SelectAst.ExprWithAlias): SqlStr =
    spaceConcat(
      renderExpr(exprWithAlias.expr),
      exprWithAlias.alias.fold(sql"")(a => sql"AS ${SqlStr.const(a)}")
    )

  protected def renderFrom(from: SelectAst.From): SqlStr = from match
    case SelectAst.From.FromQuery(query, alias) => sql"(${renderSelect(query)}) ${SqlStr.const(alias)}"
    case SelectAst.From.FromTable(table, alias) =>
      spaceConcat(SqlStr.const(table), alias.fold(sql"")(a => sql"${SqlStr.const(a)}"))
    case SelectAst.From.FromMulti(fst, snd) => sql"${renderFrom(fst)}, ${renderFrom(snd)}"
    case SelectAst.From.CrossJoin(lhs, rhs) => sql"${renderFrom(lhs)} CROSS JOIN ${renderFrom(rhs)}"
    case SelectAst.From.InnerJoin(lhs, rhs, on) =>
      sql"${renderFrom(lhs)} INNER JOIN ${renderFrom(rhs)} ON ${renderExpr(on)}"
    case SelectAst.From.LeftOuterJoin(lhs, rhs, on) =>
      sql"${renderFrom(lhs)} LEFT OUTER JOIN ${renderFrom(rhs)} ON ${renderExpr(on)}"
    case SelectAst.From.RightOuterJoin(lhs, rhs, on) =>
      sql"${renderFrom(lhs)} RIGHT OUTER JOIN ${renderFrom(rhs)} ON ${renderExpr(on)}"
    case SelectAst.From.FullOuterJoin(lhs, rhs, on) =>
      sql"${renderFrom(lhs)} FULL OUTER JOIN ${renderFrom(rhs)} ON ${renderExpr(on)}"

  protected def renderWhere(where: SqlExpr): SqlStr =
    spaceConcat(sql"WHERE", renderExpr(where))

  protected def renderGroupBy(groupBy: SelectAst.GroupBy): SqlStr =
    spaceConcat(sql"GROUP BY", groupBy.exprs.map(renderExpr).intercalate(sql", "))

  protected def renderHaving(having: SqlExpr): SqlStr =
    spaceConcat(sql"HAVING", renderExpr(having))

  protected def renderSetOperatorData(data: SelectAst.Data.SetOperatorData): SqlStr =
    val keyword = data match
      case _: SelectAst.Data.Union     => "UNION"
      case _: SelectAst.Data.Intersect => "INTERSECT"
      case _: SelectAst.Data.Except    => "EXCEPT"

    val all = if data.all then sql"ALL" else sql""

    spaceConcat(renderSelectData(data.lhs), SqlStr.const(keyword), all, renderSelectData(data.rhs))

  protected def renderSelectOrderLimit(data: SelectAst.OrderLimit): SqlStr =
    spaceConcat(data.orderBy.fold(sql"")(renderOrderBy), data.limitOffset.fold(sql"")(renderLimitOffset))

  protected def renderOrderBy(orderBy: SelectAst.OrderBy): SqlStr =
    sql"ORDER BY ${orderBy.exprs.map(renderOrderExpr).intercalate(sql", ")}"

  protected def renderOrderExpr(orderExpr: SelectAst.OrderExpr): SqlStr =
    spaceConcat(
      renderExpr(orderExpr.expr),
      renderOrderDir(orderExpr.dir),
      orderExpr.nullsOrder.fold(sql"")(renderNullsOrder)
    )

  protected def renderOrderDir(dir: SelectAst.OrderDir): SqlStr = dir match
    case SelectAst.OrderDir.Asc  => sql"ASC"
    case SelectAst.OrderDir.Desc => sql"DESC"

  protected def renderNullsOrder(nullsOrder: SelectAst.NullsOrder): SqlStr = nullsOrder match
    case SelectAst.NullsOrder.NullsFirst => sql"NULLS FIRST"
    case SelectAst.NullsOrder.NullsLast  => sql"NULLS LAST"

  protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr =
    spaceConcat(renderOffset(limitOffset), renderLimit(limitOffset).getOrElse(sql""))

  protected def renderOffset(limitOffset: SelectAst.LimitOffset): SqlStr =
    sql"OFFSET ${limitOffset.offset.as(DbType.int32)}"

  protected def renderLimit(limitOffset: SelectAst.LimitOffset): Option[SqlStr] =
    val tiesPart = if limitOffset.withTies then sql"WITH TIES" else sql"ONLY"
    limitOffset.limit.map(l => sql"FETCH NEXT ${l.as(DbType.int32)} ROWS $tiesPart")
}
