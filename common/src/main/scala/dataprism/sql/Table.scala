package dataprism.sql

import cats.Foldable
import cats.syntax.all.*
import perspective.*

case class Table[A[_[_]], Type[_]](
    tableName: String,
    columns: A[[X] =>> Column[X, Type]]
)(using val FA: ApplyKC[A], val FT: TraverseKC[A]) {

  def name: SqlStr[Type] = SqlStr.const(tableName)
}
