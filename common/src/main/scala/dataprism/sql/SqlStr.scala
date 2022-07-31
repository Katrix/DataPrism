package dataprism.sql

import cats.Monoid

case class SqlStr(str: String, args: Seq[SqlArg]) {

  def stripMargin: SqlStr = copy(str = str.stripMargin)
}
object SqlStr {
  given Monoid[SqlStr] with {
    override def empty: SqlStr = SqlStr("", Nil)

    override def combine(x: SqlStr, y: SqlStr): SqlStr =
      SqlStr(x.str + y.str, x.args ++ y.args)
  }

  def const(s: String): SqlStr = SqlStr(s, Nil)
}
