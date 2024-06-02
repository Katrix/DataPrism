package dataprism.sharedast

import cats.syntax.all.*
import dataprism.sql.*

class MariaDbAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends MySqlAstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def renderRow(row: Seq[SqlExpr[Codec]]): SqlStr[Codec] =
    sql"(${row.map(renderExpr).intercalate(sql", ")})"
}
