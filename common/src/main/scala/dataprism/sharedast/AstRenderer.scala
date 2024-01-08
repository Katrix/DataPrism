package dataprism.sharedast

import cats.syntax.all.*
import dataprism.sharedast.SqlExpr.{BinaryOperation, FunctionName, UnaryOperation}
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection,SqlDialectInspection
class AstRenderer[Type[_]](ansiTypes: AnsiTypes[Type]) {

  protected def renderUnaryOp(expr: SqlExpr[Type], op: SqlExpr.UnaryOperation): SqlStr[Type] =
    val rendered = renderExpr(expr)
    op match
      case SqlExpr.UnaryOperation.Not        => sql"(NOT $rendered)"
      case SqlExpr.UnaryOperation.Negation   => sql"(-$rendered)"
      case SqlExpr.UnaryOperation.BitwiseNot => sql"(~$rendered)"

  protected def renderBinaryOp(lhs: SqlExpr[Type], rhs: SqlExpr[Type], op: SqlExpr.BinaryOperation): SqlStr[Type] =
    val lhsr                                    = renderExpr(lhs)
    val rhsr                                    = renderExpr(rhs)
    inline def normal(op: String): SqlStr[Type] = sql"($lhsr ${SqlStr.const(op)} $rhsr)"

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

  protected def renderFunctionCall(call: SqlExpr.FunctionName, args: Seq[SqlExpr[Type]]): SqlStr[Type] =
    val rendered                               = args.map(renderExpr).intercalate(sql", ")
    inline def normal(f: String): SqlStr[Type] = sql"${SqlStr.const(f)}($rendered)"

    call match
      case SqlExpr.FunctionName.ACos  => normal("acos")
      case SqlExpr.FunctionName.ASin  => normal("asin")
      case SqlExpr.FunctionName.ATan  => normal("atan")
      case SqlExpr.FunctionName.ATan2 => normal("atan2")
      case SqlExpr.FunctionName.Cos   => normal("cos")
      case SqlExpr.FunctionName.Cot   => normal("cot")
      case SqlExpr.FunctionName.Sin   => normal("sin")
      case SqlExpr.FunctionName.Tan   => normal("tan")

      case SqlExpr.FunctionName.Abs   => normal("abs")
      case SqlExpr.FunctionName.Avg   => normal("avg")
      case SqlExpr.FunctionName.Count => normal("count")
      case SqlExpr.FunctionName.Sum   => normal("sum")
      case SqlExpr.FunctionName.Min   => normal("min")
      case SqlExpr.FunctionName.Max   => normal("max")

      case SqlExpr.FunctionName.Greatest => normal("greatest")
      case SqlExpr.FunctionName.Least    => normal("least")

      case SqlExpr.FunctionName.Ln      => normal("ln")
      case SqlExpr.FunctionName.Log     => normal("log")
      case SqlExpr.FunctionName.Log10   => normal("log10")
      case SqlExpr.FunctionName.Log2    => sql"log(2, $rendered)"
      case SqlExpr.FunctionName.Pow     => normal("power")
      case SqlExpr.FunctionName.Exp     => normal("exp")
      case SqlExpr.FunctionName.Ceiling => normal("ceil")
      case SqlExpr.FunctionName.Floor   => normal("floor")
      case SqlExpr.FunctionName.Concat  => normal("concat")

      case SqlExpr.FunctionName.Coalesce => normal("COALESCE")
      case SqlExpr.FunctionName.NullIf   => normal("NULLIF")

      case SqlExpr.FunctionName.Custom(f) => normal(f)

  protected def simplifyExpr(expr: SqlExpr[Type]): SqlExpr[Type] = expr match
    case SqlExpr.QueryRef(query, column) => SqlExpr.QueryRef(query, column)

    case SqlExpr.UnaryOp(expr, op) =>
      val se = simplifyExpr(expr)
      (op, se) match
        case (UnaryOperation.Not, SqlExpr.True())  => SqlExpr.False()
        case (UnaryOperation.Not, SqlExpr.False()) => SqlExpr.True()
        case _                                     => SqlExpr.UnaryOp(se, op)

    case SqlExpr.BinOp(lhs, rhs, op) =>
      val slhs = simplifyExpr(lhs)
      val srhs = simplifyExpr(rhs)

      (op, slhs) match
        case (BinaryOperation.BoolAnd, SqlExpr.True())  => srhs
        case (BinaryOperation.BoolAnd, SqlExpr.False()) => SqlExpr.False()
        case (BinaryOperation.BoolOr, SqlExpr.True())   => SqlExpr.True()
        case (BinaryOperation.BoolOr, SqlExpr.False())  => srhs
        case (BinaryOperation.Eq, _) if slhs == srhs && exprIsImmutable(slhs) && exprIsImmutable(srhs) => SqlExpr.True()
        case (BinaryOperation.Neq, _) if slhs == srhs && exprIsImmutable(slhs) && exprIsImmutable(srhs) =>
          SqlExpr.False()
        case _ => SqlExpr.BinOp(slhs, srhs, op)

    case SqlExpr.FunctionCall(functionCall, args) => SqlExpr.FunctionCall(functionCall, args.map(simplifyExpr))
    case SqlExpr.PreparedArgument(name, arg)      => SqlExpr.PreparedArgument(name, arg)

    case SqlExpr.Null()                 => SqlExpr.Null()
    case SqlExpr.IsNull(SqlExpr.Null()) => SqlExpr.True()
    case SqlExpr.IsNull(expr) =>
      simplifyExpr(expr) match
        case SqlExpr.Null()  => SqlExpr.True()
        case SqlExpr.False() => SqlExpr.False()
        case SqlExpr.True()  => SqlExpr.False()
        case se              => SqlExpr.IsNull(se)

    case SqlExpr.IsNotNull(SqlExpr.Null()) => SqlExpr.False()
    case SqlExpr.IsNotNull(expr) =>
      simplifyExpr(expr) match
        case SqlExpr.Null()  => SqlExpr.False()
        case SqlExpr.True()  => SqlExpr.True()
        case SqlExpr.False() => SqlExpr.True()
        case se              => SqlExpr.IsNull(se)

    case SqlExpr.InValues(expr, values) =>
      val se      = simplifyExpr(expr)
      val svalues = values.map(simplifyExpr)

      if svalues.contains(se) && exprIsImmutable(se) && svalues.find(_ == se).forall(exprIsImmutable) then
        SqlExpr.True()
      else SqlExpr.InValues(se, svalues)
    case SqlExpr.NotInValues(expr, values) =>
      val se      = simplifyExpr(expr)
      val svalues = values.map(simplifyExpr)

      if svalues.contains(se) && exprIsImmutable(se) && svalues.find(_ == se).forall(exprIsImmutable) then
        SqlExpr.False()
      else SqlExpr.NotInValues(se, svalues)

    case SqlExpr.InQuery(expr, selectAst)    => SqlExpr.InQuery(simplifyExpr(expr), selectAst)
    case SqlExpr.NotInQuery(expr, selectAst) => SqlExpr.NotInQuery(simplifyExpr(expr), selectAst)
    case SqlExpr.Cast(expr, asType)          => SqlExpr.Cast(simplifyExpr(expr), asType)
    case SqlExpr.ValueCase(matchOn, cases, orElse) =>
      val smatchOn = simplifyExpr(matchOn)
      val scases   = cases.map(t => simplifyExpr(t._1) -> simplifyExpr(t._2))

      scases
        .collectFirst { case (`smatchOn`, thenV) =>
          thenV
        }
        .getOrElse(
          if scases.isEmpty then simplifyExpr(orElse)
          else SqlExpr.ValueCase(smatchOn, scases, simplifyExpr(orElse))
        )

      SqlExpr.ValueCase(
        simplifyExpr(matchOn),
        cases.map(t => simplifyExpr(t._1) -> simplifyExpr(t._2)),
        simplifyExpr(orElse)
      )
    case SqlExpr.ConditionCase(cases, orElse) =>
      val scases = cases.map(t => simplifyExpr(t._1) -> simplifyExpr(t._2)).filter {
        case (SqlExpr.False(), _) => false
        case _                    => true
      }

      scases
        .collectFirst { case (SqlExpr.True(), thenV) =>
          thenV
        }
        .getOrElse(
          if scases.isEmpty then simplifyExpr(orElse)
          else SqlExpr.ConditionCase(scases, simplifyExpr(orElse))
        )

    case SqlExpr.SubSelect(selectAst) => SqlExpr.SubSelect(selectAst)
    case SqlExpr.QueryCount()         => SqlExpr.QueryCount()
    case SqlExpr.True()               => SqlExpr.True()
    case SqlExpr.False()              => SqlExpr.False()
    case SqlExpr.Custom(args, render) => SqlExpr.Custom(args.map(simplifyExpr), render)

  protected def functionIsImmutable(func: SqlExpr.FunctionName): Boolean = func match
    case FunctionName.Custom(f) => false
    case _                      => true

  protected def exprIsImmutable(expr: SqlExpr[Type]): Boolean = expr match
    case SqlExpr.QueryRef(_, _)                   => true
    case SqlExpr.UnaryOp(expr, _)                 => exprIsImmutable(expr)
    case SqlExpr.BinOp(lhs, rhs, _)               => exprIsImmutable(lhs) && exprIsImmutable(rhs)
    case SqlExpr.FunctionCall(functionCall, args) => args.forall(exprIsImmutable) && functionIsImmutable(functionCall)
    case SqlExpr.PreparedArgument(_, _)           => true
    case SqlExpr.Null()                           => true
    case SqlExpr.IsNull(expr)                     => exprIsImmutable(expr)
    case SqlExpr.IsNotNull(expr)                  => exprIsImmutable(expr)
    case SqlExpr.InValues(expr, values)           => exprIsImmutable(expr) && values.forall(exprIsImmutable)
    case SqlExpr.NotInValues(expr, values)        => exprIsImmutable(expr) && values.forall(exprIsImmutable)
    case SqlExpr.InQuery(_, _)                    => false
    case SqlExpr.NotInQuery(_, _)                 => false
    case SqlExpr.Cast(expr, _)                    => exprIsImmutable(expr)
    case SqlExpr.ValueCase(matchOn, cases, orElse) =>
      exprIsImmutable(matchOn) && cases.forall(t => exprIsImmutable(t._1) && exprIsImmutable(t._2)) && exprIsImmutable(
        orElse
      )
    case SqlExpr.ConditionCase(cases, orElse) =>
      cases.forall(t => exprIsImmutable(t._1) && exprIsImmutable(t._2)) && exprIsImmutable(orElse)
    case SqlExpr.SubSelect(_) => false
    case SqlExpr.QueryCount() => true
    case SqlExpr.True()       => true
    case SqlExpr.False()      => true
    case SqlExpr.Custom(_, _) => false

  protected def renderExpr(expr: SqlExpr[Type]): SqlStr[Type] = simplifyExpr(expr) match
    case SqlExpr.QueryRef(query, column) => SqlStr.const(s"$query.$column")

    case SqlExpr.UnaryOp(expr, op)   => renderUnaryOp(expr, op)
    case SqlExpr.BinOp(lhs, rhs, op) => renderBinaryOp(lhs, rhs, op)

    case SqlExpr.FunctionCall(functionCall, args) => renderFunctionCall(functionCall, args)
    case SqlExpr.PreparedArgument(_, arg)         => SqlStr("?", Seq(arg))

    case SqlExpr.Null()          => sql"NULL"
    case SqlExpr.IsNull(expr)    => sql"${renderExpr(expr)} IS NULL"
    case SqlExpr.IsNotNull(expr) => sql"${renderExpr(expr)} IS NOT NULL"

    case SqlExpr.InValues(expr, values) => sql"${renderExpr(expr)} IN (${values.map(renderExpr).intercalate(sql", ")})"
    case SqlExpr.NotInValues(expr, values) =>
      sql"${renderExpr(expr)} NOT IN (${values.map(renderExpr).intercalate(sql", ")})"
    case SqlExpr.InQuery(expr, ast)    => sql"${renderExpr(expr)} IN (${renderSelect(ast)})"
    case SqlExpr.NotInQuery(expr, ast) => sql"${renderExpr(expr)} NOT IN (${renderSelect(ast)})"

    case SqlExpr.Cast(expr, asType) => sql"(CAST(${renderExpr(expr)} AS ${SqlStr.const(asType)}))"

    case SqlExpr.ValueCase(matchOn, cases, orElse) =>
      sql"CASE ${renderExpr(matchOn)} ${cases.toVector
          .map(t => sql"WHEN ${renderExpr(t._1)} THEN ${renderExpr(t._2)}")
          .intercalate(sql" ")} ELSE ${renderExpr(orElse)} END"
    case SqlExpr.ConditionCase(cases, orElse) =>
      sql"CASE ${cases.toVector
          .map(t => sql"WHEN ${renderExpr(t._1)} THEN ${renderExpr(t._2)}")
          .intercalate(sql" ")} ELSE ${renderExpr(orElse)} END"

    case SqlExpr.SubSelect(selectAst) => sql"(${renderSelect(selectAst)})"

    case SqlExpr.QueryCount()         => sql"COUNT(*)"
    case SqlExpr.True()               => sql"TRUE"
    case SqlExpr.False()              => sql"FALSE"
    case SqlExpr.Custom(args, render) => render(args.map(renderExpr))

  protected def spaceConcat(args: SqlStr[Type]*): SqlStr[Type] =
    args.filter(_.nonEmpty).intercalate(sql" ")

  def renderUpdate(
      columnNames: List[SqlStr[Type]],
      valuesAst: SelectAst[Type],
      returningExprs: List[SqlExpr[Type]]
  ): SqlStr[Type] =
    val (table, alias, fromV, where, exprs) = valuesAst match {
      case SelectAst.SelectFrom(
            None,
            exprs,
            Some(SelectAst.From.FromMulti(SelectAst.From.FromTable(table, alias), usingV)),
            where,
            None,
            None,
            None,
            None,
            None
          ) =>
        (table, alias, sql"FROM ${renderFrom(usingV)}", where, exprs)

      case SelectAst.SelectFrom(
            None,
            exprs,
            Some(SelectAst.From.FromTable(table, alias)),
            where,
            None,
            None,
            None,
            None,
            None
          ) =>
        (table, alias, sql"", where, exprs)

      case _ =>
        // TODO: Enforce statically in the API
        throw new IllegalArgumentException("Can't use any other operator than from stuff and where with renderUpdate")
    }

    spaceConcat(
      sql"UPDATE ",
      SqlStr.const(table),
      sql"AS",
      alias.fold(sql"")(a => sql"AS ${SqlStr.const(a)}"),
      sql"SET",
      columnNames.zip(exprs).map((col, e) => sql"$col = ${renderExpr(e.expr)}").intercalate(sql", "),
      fromV,
      where.fold(sql"")(renderWhere),
      if returningExprs.isEmpty then sql"" else sql"RETURNING ${returningExprs.map(renderExpr).intercalate(sql", ")}"
    )

  def renderInsert(
      table: SqlStr[Type],
      columns: List[SqlStr[Type]],
      values: SelectAst[Type],
      conflictOn: List[SqlStr[Type]],
      onConflict: List[(SqlStr[Type], SqlExpr[Type])],
      returning: List[SqlExpr[Type]]
  ): SqlStr[Type] =
    // Fix the AST so that an alias isn't included for VALUES, as we then end up with (VALUES) AS ... (...)
    val fixedValues = values match
      case data: SelectAst.Values[Type] => data.copy(alias = None, columnAliases = None)
      case _                            => values

    spaceConcat(
      sql"INSERT INTO",
      table,
      sql"(",
      columns.intercalate(sql", "),
      sql")",
      renderSelect(fixedValues),
      if onConflict.isEmpty then sql""
      else
        val conflictSets = onConflict.map((col, e) => sql"$col = ${renderExpr(e)}").intercalate(sql", ")
        sql"ON CONFLICT (${conflictOn.intercalate(sql", ")}) DO UPDATE SET $conflictSets"
      ,
      if returning.isEmpty then sql"" else sql"RETURNING ${returning.map(renderExpr).intercalate(sql",  ")}"
    )

  def renderDelete(query: SelectAst[Type], returning: Boolean): SqlStr[Type] =
    val (table, alias, usingV, where, exprs) = query match {
      case SelectAst.SelectFrom(
            None,
            exprs,
            Some(SelectAst.From.FromMulti(SelectAst.From.FromTable(table, alias), usingV)),
            where,
            None,
            None,
            None,
            None,
            None
          ) =>
        (table, alias, sql"USING ${renderFrom(usingV)}", where, exprs)

      case SelectAst.SelectFrom(
            None,
            exprs,
            Some(SelectAst.From.FromTable(table, alias)),
            where,
            None,
            None,
            None,
            None,
            None
          ) =>
        (table, alias, sql"", where, exprs)

      case _ =>
        // TODO: Enforce statically in the API
        throw new IllegalArgumentException("Can't use any other operator than from stuff and where with renderDelete")
    }

    spaceConcat(
      sql"DELETE FROM",
      SqlStr.const(table),
      alias.fold(sql"")(a => sql"AS ${SqlStr.const(a)}"),
      usingV,
      where.fold(sql"")(renderWhere),
      if returning then sql"RETURNING ${exprs.map(renderExprWithAlias).intercalate(sql", ")}" else sql""
    )
  end renderDelete

  def renderSelect(data: SelectAst[Type]): SqlStr[Type] = data match
    case d: SelectAst.SelectFrom[Type]  => renderSelectFrom(d)
    case d: SelectAst.Values[Type]      => renderSelectValues(d)
    case d: SelectAst.SetOperator[Type] => renderSetOperatorData(d)

  protected def renderSelectFrom(data: SelectAst.SelectFrom[Type]): SqlStr[Type] =
    val distinct    = data.distinct.fold(sql"")(renderDistinct)
    val exprs       = data.selectExprs.map(renderExprWithAlias).intercalate(sql", ")
    val from        = data.from.fold(sql"")(f => sql"FROM ${renderFrom(f)}")
    val where       = data.where.fold(sql"")(renderWhere)
    val groupBy     = data.groupBy.fold(sql"")(renderGroupBy)
    val having      = data.having.fold(sql"")(renderHaving)
    val orderBy     = data.orderBy.fold(sql"")(renderOrderBy)
    val limitOffset = data.limitOffset.fold(sql"")(renderLimitOffset)

    spaceConcat(sql"SELECT", distinct, exprs, from, where, groupBy, having, orderBy, limitOffset)

  protected def renderSelectValues(values: SelectAst.Values[Type]): SqlStr[Type] =
    val res = spaceConcat(
      sql"VALUES ",
      values.valueExprs.map(v => sql"(${v.map(renderExpr).intercalate(sql", ")})").intercalate(sql", "),
      values.alias.fold(sql"")(a => sql"AS ${SqlStr.const(a)}"),
      values.columnAliases.fold(sql"")(as => sql"(${as.map(SqlStr.const).intercalate(sql", ")})")
    )
    if values.alias.isDefined || values.columnAliases.isDefined then sql"($res)" else res

  protected def renderDistinct(distinct: SelectAst.Distinct[Type]): SqlStr[Type] =
    spaceConcat(
      sql"DISTINCT",
      if distinct.on.isEmpty then sql"" else sql"ON",
      distinct.on.map(renderExpr).intercalate(sql", ")
    )

  protected def renderExprWithAlias(exprWithAlias: SelectAst.ExprWithAlias[Type]): SqlStr[Type] =
    spaceConcat(
      renderExpr(exprWithAlias.expr),
      exprWithAlias.alias.fold(sql"")(a => sql"AS ${SqlStr.const(a)}")
    )

  protected def renderFrom(from: SelectAst.From[Type]): SqlStr[Type] = from match
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

  protected def renderWhere(where: SqlExpr[Type]): SqlStr[Type] =
    val e = simplifyExpr(where)
    if e == SqlExpr.True() then sql""
    else spaceConcat(sql"WHERE", renderExpr(e))

  protected def renderGroupBy(groupBy: SelectAst.GroupBy[Type]): SqlStr[Type] =
    spaceConcat(sql"GROUP BY", groupBy.exprs.map(renderExpr).intercalate(sql", "))

  protected def renderHaving(having: SqlExpr[Type]): SqlStr[Type] =
    val e = simplifyExpr(having)
    if e == SqlExpr.True() then sql""
    else spaceConcat(sql"HAVING", renderExpr(e))

  protected def renderSetOperatorData(data: SelectAst.SetOperator[Type]): SqlStr[Type] =
    val keyword = data match
      case _: SelectAst.Union[Type]     => "UNION"
      case _: SelectAst.Intersect[Type] => "INTERSECT"
      case _: SelectAst.Except[Type]    => "EXCEPT"

    val all = if data.all then sql"ALL" else sql""

    spaceConcat(sql"(${renderSelect(data.lhs)})", SqlStr.const(keyword), all, sql"(${renderSelect(data.rhs)})")

  protected def renderOrderBy(orderBy: SelectAst.OrderBy[Type]): SqlStr[Type] =
    sql"ORDER BY ${orderBy.exprs.map(renderOrderExpr).intercalate(sql", ")}"

  protected def renderOrderExpr(orderExpr: SelectAst.OrderExpr[Type]): SqlStr[Type] =
    spaceConcat(
      renderExpr(orderExpr.expr),
      renderOrderDir(orderExpr.dir),
      orderExpr.nullsOrder.fold(sql"")(renderNullsOrder)
    )

  protected def renderOrderDir(dir: SelectAst.OrderDir): SqlStr[Type] = dir match
    case SelectAst.OrderDir.Asc  => sql"ASC"
    case SelectAst.OrderDir.Desc => sql"DESC"

  protected def renderNullsOrder(nullsOrder: SelectAst.NullsOrder): SqlStr[Type] = nullsOrder match
    case SelectAst.NullsOrder.NullsFirst => sql"NULLS FIRST"
    case SelectAst.NullsOrder.NullsLast  => sql"NULLS LAST"

  protected def renderLimitOffset(limitOffset: SelectAst.LimitOffset): SqlStr[Type] =
    spaceConcat(renderOffset(limitOffset), renderLimit(limitOffset).getOrElse(sql""))

  protected def renderOffset(limitOffset: SelectAst.LimitOffset): SqlStr[Type] =
    sql"OFFSET ${limitOffset.offset.asArg(ansiTypes.integer)}"

  protected def renderLimit(limitOffset: SelectAst.LimitOffset): Option[SqlStr[Type]] =
    val tiesPart = if limitOffset.withTies then sql"WITH TIES" else sql"ONLY"
    limitOffset.limit.map(l => sql"FETCH NEXT ${l.asArg(ansiTypes.integer)} ROWS $tiesPart")
}
