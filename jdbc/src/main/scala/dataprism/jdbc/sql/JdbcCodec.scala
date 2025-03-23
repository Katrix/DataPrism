package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.reflect.ClassTag
import scala.util.NotGiven
import cats.Invariant
import dataprism.sql.{Nullable, ResourceManager, SqlNull}

class JdbcCodec[A] private (
    val name: String,
    val get: ResourceManager ?=> (ResultSet, Int, Connection) => Either[String, A],
    val set: ResourceManager ?=> (PreparedStatement, Int, A, Connection) => Unit,
    val box: A => AnyRef
):

  def imap[B](f: A => B)(g: B => A): JdbcCodec[B] =
    JdbcCodec(name, (a, b, c) => this.get(a, b, c).map(f), (a, b, c, d) => this.set(a, b, g(c), d), b => box(g(b)))

  def eimap[B](f: A => Either[String, B])(g: B => A): JdbcCodec[B] =
    JdbcCodec(name, (a, b, c) => this.get(a, b, c).flatMap(f), (a, b, c, d) => this.set(a, b, g(c), d), b => box(g(b)))

  def get[B](using ev1: A <:< (B | SqlNull), ev2: NotGiven[B <:< SqlNull], ev3: B <:< A): JdbcCodec[B] = {
    imap[B](a => Nullable.syntax(ev1(a)).fold(throw new Exception("Unexpected null value"))(identity))(b => ev3(b))
  }

object JdbcCodec {
  given Invariant[JdbcCodec] with
    override def imap[A, B](fa: JdbcCodec[A])(f: A => B)(g: B => A): JdbcCodec[B] = fa.imap(f)(g)

  def simple[A](
      name: String,
      get: (ResultSet, Int) => A | SqlNull,
      set: (PreparedStatement, Int, A | SqlNull) => Unit,
      box: A => AnyRef = (_: A).asInstanceOf[AnyRef]
  )(using NotGiven[SqlNull <:< A]): JdbcCodec[A | SqlNull] =
    JdbcCodec(
      name,
      (a, b, _) => Right(get(a, b)),
      (a, b, c, _) => set(a, b, c),
      a => Nullable.syntax(a).map(box)(using NotGiven.default).orSqlNull
    )

  def byClass[A <: AnyRef](name: String, sqlType: Int)(using c: ClassTag[A]): JdbcCodec[A | SqlNull] = {
    simple[A](
      name,
      (a, b) => Option(a.getObject(b, c.runtimeClass.asInstanceOf[Class[A]])).getOrElse(SqlNull),
      (a, b, c) => a.setObject(b, Nullable.syntax(c).orNull, sqlType)
    )
  }

  def withWasNullCheck[A](
      name: String,
      sqlType: Int,
      get: (ResultSet, Int) => A,
      set: (PreparedStatement, Int, A) => Unit,
      box: A => AnyRef = (_: A).asInstanceOf[AnyRef]
  ): JdbcCodec[A | SqlNull] = {
    simple(
      name,
      (rs, i) => {
        val r       = get(rs, i)
        val wasNull = rs.wasNull() || r == null // Seems to sometimes lie...
        if wasNull then SqlNull else r
      },
      (ps, i, vo) => Nullable.syntax(vo).fold(ps.setNull(i, sqlType, name))(v => set(ps, i, v)),
      box
    )
  }

  def failable[A](
      name: String,
      get: (ResultSet, Int) => Either[String, A | SqlNull],
      set: (PreparedStatement, Int, A | SqlNull) => Unit,
      box: A => AnyRef = (_: A).asInstanceOf[AnyRef]
  )(using NotGiven[SqlNull <:< A]): JdbcCodec[A | SqlNull] = {
    JdbcCodec(
      name,
      (a, b, _) => get(a, b),
      (a, b, c, _) => set(a, b, c),
      v => Nullable.syntax(v).map(box)(using NotGiven.default).orSqlNull
    )
  }

  def withConnection[A](
      name: String,
      get: ResourceManager ?=> (ResultSet, Int, Connection) => Either[String, A | SqlNull],
      set: ResourceManager ?=> (PreparedStatement, Int, A | SqlNull, Connection) => Unit,
      box: A => AnyRef = (_: A).asInstanceOf[AnyRef]
  )(using NotGiven[SqlNull <:< A]): JdbcCodec[A | SqlNull] = {
    JdbcCodec(name, get, set, v => Nullable.syntax(v).map(box)(using NotGiven.default).orSqlNull)
  }
}
