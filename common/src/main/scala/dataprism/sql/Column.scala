package dataprism.sql

case class Column[A](nameStr: String, tpe: DbType[A]) {
  def name: SqlStr = SqlStr(nameStr, Nil)

  def typeName: SqlStr = SqlStr(tpe.name, Nil)
}
