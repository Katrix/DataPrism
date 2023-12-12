package dataprism.sql

trait SqlArg[+Type[_]] {
  type A
  def value: A
  def tpe: Type[A]
}
object SqlArg {
  case class SqlArgObj[A0, Type[_]](value: A0, tpe: Type[A0]) extends SqlArg[Type] {
    type A = A0
  }
}
