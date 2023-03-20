package dataprism.sql

import cats.Foldable
import cats.syntax.all.*
import perspective.*

case class Table[A[_[_]]](
    tableName: String,
    columns: A[Column]
)(using val FA: ApplyKC[A], val FT: TraverseKC[A]) {

  def name: SqlStr = SqlStr(tableName, Nil)
}
