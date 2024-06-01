package dataprism.skunk.sql

import dataprism.sharedast.{PostgresAstRenderer, SelectAst, SqlExpr}
import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class PostgresSkunkAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends PostgresAstRenderer[Codec](ansiTypes, getCodecTypeName) {

  private val replaceStr = "DATAPRISM_SKUNK_REPLACE_WITH_ARG_NUM"

  private def postProcessAssignArgIndices(str: SqlStr[Codec]): SqlStr[Codec] =
    val query = str.str

    val buf = StringBuffer(query)
    var subtractIdxAmount = 0
    replaceStr.r.findAllMatchIn(query).zipWithIndex.foreach { case (m, idx) =>
      buf.replace(m.start - subtractIdxAmount, m.end - subtractIdxAmount, (idx + 1).toString)
      subtractIdxAmount += replaceStr.length
      subtractIdxAmount -= (idx + 1).toString.length
    }

    SqlStr(buf.toString, str.args)

  override def renderInsert(
      table: SqlStr[Codec],
      columns: List[SqlStr[Codec]],
      values: SelectAst[Codec],
      conflictOn: List[SqlStr[Codec]],
      onConflict: List[(SqlStr[Codec], SqlExpr[Codec])],
      returning: List[SqlExpr[Codec]]
  ): SqlStr[Codec] = postProcessAssignArgIndices(super.renderInsert(table, columns, values, conflictOn, onConflict, returning))

  override def renderUpdate(
      columnNames: List[SqlStr[Codec]],
      valuesAst: SelectAst[Codec],
      returningExprs: List[SqlExpr[Codec]]
  ): SqlStr[Codec] =
    postProcessAssignArgIndices(super.renderUpdate(columnNames, valuesAst, returningExprs))

  override def renderDelete(query: SelectAst[Codec], returning: Boolean): SqlStr[Codec] =
    postProcessAssignArgIndices(super.renderDelete(query, returning))

  override def renderSelectStatement(data: SelectAst[Codec]): SqlStr[Codec] =
    postProcessAssignArgIndices(super.renderSelectStatement(data))

  override protected def renderPreparedArgument(arg: SqlExpr.PreparedArgument[Codec]): SqlStr[Codec] =
    SqlStr(s"($$$replaceStr::${arg.arg.codec.name})", Seq(arg.arg))
}
