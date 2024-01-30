package dataprism.sharedast

import java.sql.SQLException

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class SqliteAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec]) extends AstRenderer[Codec](ansiTypes) {

  override protected def renderFrom(from: SelectAst.From[Codec]): SqlStr[Codec] = from match
    case SelectAst.From.FromQuery(_, _, true) => throw new SQLException("H2 does not support lateral")
    case _                                    => super.renderFrom(from)
}
