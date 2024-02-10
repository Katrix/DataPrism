package dataprism.sql

import cats.syntax.all.*
import perspective.*

case class Table[Codec[_], A[_[_]]](
    tableName: String,
    columns: A[[X] =>> Column[Codec, X]]
)(using val FA: ApplyKC[A], val FT: TraverseKC[A]) {

  def name: SqlStr[Codec] = SqlStr.const(tableName)

  def types: A[[X] =>> SelectedType[Codec, X]] = FA.mapK(columns)([X] => (col: Column[Codec, X]) => col.tpe)
}
