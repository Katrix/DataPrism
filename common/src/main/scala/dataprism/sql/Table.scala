package dataprism.sql

import cats.Foldable
import cats.syntax.all.*
import perspective.*

case class Table[A[_[_]]](
    tableName: String,
    columns: A[Column]
)(using val FA: ApplyKC[A], val FT: TraverseKC[A]) {

  def name: SqlStr = SqlStr(tableName, Nil)

  def columnsFrag: SqlStr =
    val columnNames: List[SqlStr] = columns.foldMapK([X] => (column: Column[X]) => List(column.name))
    columnNames.intercalate(sql", ")

  def columnsFragAdvanced(tableName: SqlStr, columnPrefix: SqlStr): SqlStr =
    val columnNames: List[SqlStr] =
      columns.foldMapK([X] => (column: Column[X]) => List(sql"$tableName.$columnPrefix${column.name}"))
    columnNames.intercalate(sql", ")

  def selectFrag: SqlStr = sql"SELECT $columnsFrag FROM $name"

  def valueFrag(values: A[Id]*): SqlStr =
    values
      .map { value =>
        sql"(${columns
            .map2Const(value)([X] => (column: Column[X], value: X) => sql"${value.as(column.tpe)}")
            .toListK
            .intercalate(sql", ")})"
      }
      .intercalate(sql", ")

  def updateFrag(values: A[Id]): SqlStr =
    columns
      .map2Const(values)([X] => (column: Column[X], value: X) => sql"$column = ${value.as(column.tpe)}")
      .toListK
      .intercalate(sql", ")
}
object Table {
  def tableDbTypes[A[_[_]]: FunctorKC](table: Table[A]): A[DbType] =
    table.columns.mapK([A] => (column: Column[A]) => column.tpe)
}
