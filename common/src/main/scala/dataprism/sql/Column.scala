package dataprism.sql

case class Column[A, Codec[_]](nameStr: String, tpe: SelectedType[A, Codec]):
  def name: SqlStr[Codec] = SqlStr.const(nameStr)
object Column:
  def apply[A, Codec[_]](nameStr: String, tpe: NullabilityTypeChoice[A, Codec]) = new Column(nameStr, tpe.notNull)
