package dataprism.skunk.sql

import scala.collection.immutable.Seq

import cats.data.State
import cats.syntax.all.*
import dataprism.sharedast.{PostgresAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class PostgresSkunkAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends PostgresAstRenderer[Codec](ansiTypes, getCodecTypeName) {

  private def assignArgumentIndicesExpr(expr: SqlExpr[Codec]): State[Int, SqlExpr[Codec]] = expr match
    case SqlExpr.QueryRef(_, _)    => State.pure(expr)
    case SqlExpr.UnaryOp(expr, op, tpe) => assignArgumentIndicesExpr(expr).map(SqlExpr.UnaryOp(_, op, tpe))
    case SqlExpr.BinOp(lhs, rhs, op, tpe) =>
      assignArgumentIndicesExpr(lhs).map2(assignArgumentIndicesExpr(rhs))((l, r) => SqlExpr.BinOp(l, r, op, tpe))
    case SqlExpr.FunctionCall(functionCall, args, tpe) =>
      args.traverse(assignArgumentIndicesExpr).map(SqlExpr.FunctionCall(functionCall, _, tpe))
    case SqlExpr.PreparedArgument(_, arg) => State(i => (i + 1, SqlExpr.PreparedArgument(Some(i.toString), arg)))
    case SqlExpr.Null()                   => State.pure(SqlExpr.Null())
    case SqlExpr.IsNull(expr)             => assignArgumentIndicesExpr(expr).map(SqlExpr.IsNull.apply)
    case SqlExpr.IsNotNull(expr)          => assignArgumentIndicesExpr(expr).map(SqlExpr.IsNotNull.apply)
    case SqlExpr.InValues(expr, values) =>
      assignArgumentIndicesExpr(expr).map2(values.traverse(assignArgumentIndicesExpr))((e, vs) =>
        SqlExpr.InValues(e, vs)
      )
    case SqlExpr.NotInValues(expr, values) =>
      assignArgumentIndicesExpr(expr).map2(values.traverse(assignArgumentIndicesExpr))((e, vs) =>
        SqlExpr.NotInValues(e, vs)
      )
    case SqlExpr.InQuery(expr, selectAst) =>
      assignArgumentIndicesExpr(expr).map2(assignArgumentIndicesSelect(selectAst))((e, s) => SqlExpr.InQuery(e, s))
    case SqlExpr.NotInQuery(expr, selectAst) =>
      assignArgumentIndicesExpr(expr).map2(assignArgumentIndicesSelect(selectAst))((e, s) => SqlExpr.NotInQuery(e, s))
    case SqlExpr.Cast(expr, asType) => assignArgumentIndicesExpr(expr).map(e => SqlExpr.Cast(e, asType))
    case SqlExpr.ValueCase(matchOn, cases, orElse) =>
      (
        assignArgumentIndicesExpr(matchOn),
        (cases: Seq[(SqlExpr[Codec], SqlExpr[Codec])])
          .traverse(t => assignArgumentIndicesExpr(t._1).product(assignArgumentIndicesExpr(t._2))),
        assignArgumentIndicesExpr(orElse)
      ).mapN((m, cs, or) => SqlExpr.ValueCase(m, cs.toIndexedSeq, or))
    case SqlExpr.ConditionCase(cases, orElse) =>
      (cases: Seq[(SqlExpr[Codec], SqlExpr[Codec])])
        .traverse(t => assignArgumentIndicesExpr(t._1).product(assignArgumentIndicesExpr(t._2)))
        .map2(assignArgumentIndicesExpr(orElse))((cs, or) => SqlExpr.ConditionCase(cs.toIndexedSeq, or))
    case SqlExpr.SubSelect(selectAst) => assignArgumentIndicesSelect(selectAst).map(SqlExpr.SubSelect.apply)
    case SqlExpr.QueryCount()         => State.pure(SqlExpr.QueryCount())
    case SqlExpr.True()               => State.pure(SqlExpr.True())
    case SqlExpr.False()              => State.pure(SqlExpr.False())
    case SqlExpr.Custom(args, render) => args.traverse(assignArgumentIndicesExpr).map(as => SqlExpr.Custom(as, render))

  private def assignArgumentIndicesFrom(from: SelectAst.From[Codec]): State[Int, SelectAst.From[Codec]] = from match
    case SelectAst.From.FromTable(table, alias) => State.pure(SelectAst.From.FromTable(table, alias))
    case SelectAst.From.FromQuery(selectAst, alias, lateral) =>
      assignArgumentIndicesSelect(selectAst).map(select => SelectAst.From.FromQuery(select, alias, lateral))
    case SelectAst.From.FromMulti(fst, snd) =>
      assignArgumentIndicesFrom(fst).map2(assignArgumentIndicesFrom(snd))((l, r) => SelectAst.From.FromMulti(l, r))
    case SelectAst.From.CrossJoin(lhs, rhs) =>
      assignArgumentIndicesFrom(lhs).map2(assignArgumentIndicesFrom(rhs))((l, r) => SelectAst.From.CrossJoin(l, r))
    case SelectAst.From.InnerJoin(lhs, rhs, on) =>
      (assignArgumentIndicesFrom(lhs), assignArgumentIndicesFrom(rhs), assignArgumentIndicesExpr(on)).mapN((l, r, o) =>
        SelectAst.From.InnerJoin(l, r, o)
      )

    case SelectAst.From.LeftOuterJoin(lhs, rhs, on) =>
      (assignArgumentIndicesFrom(lhs), assignArgumentIndicesFrom(rhs), assignArgumentIndicesExpr(on)).mapN((l, r, o) =>
        SelectAst.From.LeftOuterJoin(l, r, o)
      )

    case SelectAst.From.RightOuterJoin(lhs, rhs, on) =>
      (assignArgumentIndicesFrom(lhs), assignArgumentIndicesFrom(rhs), assignArgumentIndicesExpr(on)).mapN((l, r, o) =>
        SelectAst.From.RightOuterJoin(l, r, o)
      )

    case SelectAst.From.FullOuterJoin(lhs, rhs, on) =>
      (assignArgumentIndicesFrom(lhs), assignArgumentIndicesFrom(rhs), assignArgumentIndicesExpr(on)).mapN((l, r, o) =>
        SelectAst.From.FullOuterJoin(l, r, o)
      )

  private def assignArgumentIndicesSelect(selectAst: SelectAst[Codec]): State[Int, SelectAst[Codec]] = selectAst match
    case SelectAst.SelectFrom(distinct, selectExprs, from, where, groupBy, having, orderBy, limitOffset, locks) =>
      for
        newDistinct <- distinct.traverse(d => d.on.traverse(assignArgumentIndicesExpr).map(SelectAst.Distinct(_)))
        newExprs <- selectExprs.traverse(e =>
          assignArgumentIndicesExpr(e.expr).map(newE => SelectAst.ExprWithAlias(newE, e.alias))
        )
        newFrom  <- from.traverse(assignArgumentIndicesFrom)
        newWhere <- where.traverse(assignArgumentIndicesExpr)
        newGroupBy <- groupBy.traverse(g =>
          g.exprs.traverse(assignArgumentIndicesExpr).map(es => SelectAst.GroupBy(es))
        )
        newHaving <- having.traverse(assignArgumentIndicesExpr)
        newOrderBy <- orderBy.traverse(order =>
          order.exprs
            .traverse(oe => assignArgumentIndicesExpr(oe.expr).map(e => SelectAst.OrderExpr(e, oe.dir, oe.nullsOrder)))
            .map(es => SelectAst.OrderBy(es))
        )
        newLimitOffset <- limitOffset.traverse(lo =>
          lo.offset
            .traverse(assignArgumentIndicesExpr)
            .map2(lo.limit.traverse(assignArgumentIndicesExpr))((o, l) => SelectAst.LimitOffset(l, o, lo.withTies))
        )
      yield SelectAst.SelectFrom(
        newDistinct,
        newExprs,
        newFrom,
        newWhere,
        newGroupBy,
        newHaving,
        newOrderBy,
        newLimitOffset,
        locks
      )
    case SelectAst.Union(lhs, rhs, all) =>
      assignArgumentIndicesSelect(lhs).map2(assignArgumentIndicesSelect(rhs))((l, r) => SelectAst.Union(l, r, all))
    case SelectAst.Intersect(lhs, rhs, all) =>
      assignArgumentIndicesSelect(lhs).map2(assignArgumentIndicesSelect(rhs))((l, r) => SelectAst.Intersect(l, r, all))
    case SelectAst.Except(lhs, rhs, all) =>
      assignArgumentIndicesSelect(lhs).map2(assignArgumentIndicesSelect(rhs))((l, r) => SelectAst.Except(l, r, all))
    case SelectAst.Values(valueExprs, alias, columnAliases) =>
      valueExprs
        .traverse(_.traverse(assignArgumentIndicesExpr))
        .map(newValues => SelectAst.Values(newValues, alias, columnAliases))

  override def renderInsert(
      table: SqlStr[Codec],
      columns: List[SqlStr[Codec]],
      values: SelectAst[Codec],
      conflictOn: List[SqlStr[Codec]],
      onConflict: List[(SqlStr[Codec], SqlExpr[Codec])],
      returning: List[SqlExpr[Codec]]
  ): SqlStr[Codec] =
    val run = for
      newValues     <- assignArgumentIndicesSelect(values)
      newOnConflict <- onConflict.traverse((str, expr) => assignArgumentIndicesExpr(expr).map(str -> _))
    yield super.renderInsert(table, columns, newValues, conflictOn, newOnConflict, returning)
    run.runA(0).value

  override def renderUpdate(
      columnNames: List[SqlStr[Codec]],
      valuesAst: SelectAst[Codec],
      returningExprs: List[SqlExpr[Codec]]
  ): SqlStr[Codec] =
    val run = for
      newValues         <- assignArgumentIndicesSelect(valuesAst)
      newReturningExprs <- returningExprs.traverse(assignArgumentIndicesExpr)
    yield super.renderUpdate(columnNames, newValues, newReturningExprs)
    run.runA(0).value

  override def renderDelete(query: SelectAst[Codec], returning: Boolean): SqlStr[Codec] =
    assignArgumentIndicesSelect(query).map(newQuery => super.renderDelete(newQuery, returning)).runA(0).value

  override def renderSelectStatement(data: SelectAst[Codec]): SqlStr[Codec] =
    assignArgumentIndicesSelect(data).map(super.renderSelectStatement).runA(0).value

  override protected def renderPreparedArgument(arg: SqlExpr.PreparedArgument[Codec]): SqlStr[Codec] =
    val nameInt = arg.name
      .getOrElse(sys.error(s"Expected name to be present in $arg"))
      .toIntOption
      .getOrElse(sys.error("Expected name to be an integer")) + 1

    SqlStr(s"($$$nameInt::${arg.arg.codec.name})", Seq(arg.arg))
}
