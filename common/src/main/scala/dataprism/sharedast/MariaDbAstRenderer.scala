package dataprism.sharedast

import dataprism.sql.*
import cats.syntax.all.*

class MariaDbAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
    extends MySqlAstRenderer[Codec](ansiTypes, getCodecTypeName) {

  override protected def renderRow(row: Seq[SqlExpr[Codec]]): SqlStr[Codec] =
    sql"(${row.map(renderExpr).intercalate(sql", ")})"
}
