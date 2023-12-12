package dataprism.sql

case class Column[A, Type[_]](nameStr: String, tpe: Type[A]) {
  def name: SqlStr[Type] = SqlStr.const(nameStr)
}
