package dataprism.sql

case class Column[Codec[_], A](nameStr: String, tpe: SelectedType[Codec, A]):
  def name: SqlStr[Codec] = SqlStr.const(nameStr)
