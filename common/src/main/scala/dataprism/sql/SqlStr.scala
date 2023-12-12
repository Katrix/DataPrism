package dataprism.sql

import cats.Monoid

case class SqlStr[+Type[_]](str: String, args: Seq[SqlArg[Type]]):

  def isEmpty: Boolean  = str.isEmpty

  def nonEmpty: Boolean = str.nonEmpty

  def stripMargin: SqlStr[Type] = copy(str = str.stripMargin)

object SqlStr:
  given [Type[_]]: Monoid[SqlStr[Type]] with 
    override def empty: SqlStr[Type] = SqlStr("", Nil)

    override def combine(x: SqlStr[Type], y: SqlStr[Type]): SqlStr[Type] =
      SqlStr(x.str + y.str, x.args ++ y.args)
  end given

  given nothingSqlStrMonoid: Monoid[SqlStr[Nothing]] with
    override def empty: SqlStr[Nothing] = SqlStr("", Nil)

    override def combine(x: SqlStr[Nothing], y: SqlStr[Nothing]): SqlStr[Nothing] =
      SqlStr(x.str + y.str, x.args ++ y.args)
  end nothingSqlStrMonoid

  def const[Type[_]](s: String): SqlStr[Type] = SqlStr(s, Nil)
