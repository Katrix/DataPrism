package dataprism.sql

trait SqlArg {
  type A
  def value: A
  def tpe: DbType[A]
}
object SqlArg {
  case class SqlArgObj[A0](value: A0, tpe: DbType[A0]) extends SqlArg {
    type A = A0
  }
}
