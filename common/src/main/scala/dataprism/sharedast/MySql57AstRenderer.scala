package dataprism.sharedast

import dataprism.sql.{AnsiTypes, SqlStr}

class MySql57AstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends MySqlAstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def parenthesisAroundSetOps: Boolean = false

  // Sigh...
  override protected def renderSelectValues(values: SelectAst.Values[Codec]): SqlStr[Codec] = {
    val selects = values.valueExprs.map { row =>
      SelectAst.SelectFrom(
        distinct = None,
        selectExprs = row
          .zip(values.columnAliases.map(_.map(Some.apply)).getOrElse(row.map(_ => None)))
          .map((expr, alias) => SelectAst.ExprWithAlias(expr, alias)),
        from = None,
        where = None,
        groupBy = None,
        having = None,
        orderBy = None,
        limitOffset = None,
        locks = None
      )
    }

    renderSelect(
      selects.tail.foldLeft(selects.head: SelectAst[Codec])((acc, from) => SelectAst.Union(acc, from, all = true))
    )
  }
}
