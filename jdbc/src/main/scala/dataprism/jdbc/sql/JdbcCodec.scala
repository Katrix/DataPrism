package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.quoted.ToExpr.ClassTagToExpr
import scala.reflect.ClassTag
import scala.util.Using

import cats.Invariant

class JdbcCodec[A] private (
    val name: String,
    val get: Using.Manager ?=> (ResultSet, Int, Connection) => Either[String, A],
    val set: Using.Manager ?=> (PreparedStatement, Int, A, Connection) => Unit
):

  def imap[B](f: A => B)(g: B => A): JdbcCodec[B] =
    JdbcCodec(name, (a, b, c) => this.get(a, b, c).map(f), (a, b, c, d) => this.set(a, b, g(c), d))

  def eimap[B](f: A => Either[String, B])(g: B => A): JdbcCodec[B] =
    JdbcCodec(name, (a, b, c) => this.get(a, b, c).flatMap(f), (a, b, c, d) => this.set(a, b, g(c), d))

  def get[B](using ev: A =:= Option[B]): JdbcCodec[B] = imap[B](_.get)(v => ev.flip(Some(v)))

object JdbcCodec {
  given Invariant[JdbcCodec] with
    override def imap[A, B](fa: JdbcCodec[A])(f: A => B)(g: B => A): JdbcCodec[B] = fa.imap(f)(g)

  def simple[A](
      name: String,
      get: (ResultSet, Int) => Option[A],
      set: (PreparedStatement, Int, Option[A]) => Unit
  ): JdbcCodec[Option[A]] =
    JdbcCodec(name, (a, b, _) => Right(get(a, b)), (a, b, c, _) => set(a, b, c))

  def byClass[A <: AnyRef](name: String, sqlType: Int)(using c: ClassTag[A]): JdbcCodec[Option[A]] =
    simple[A](
      name,
      (a, b) => Option(a.getObject(b, c.runtimeClass.asInstanceOf[Class[A]])),
      (a, b, c) => a.setObject(b, c.orNull, sqlType)
    )

  def failable[A](
      name: String,
      get: (ResultSet, Int) => Either[String, Option[A]],
      set: (PreparedStatement, Int, Option[A]) => Unit
  ): JdbcCodec[Option[A]] =
    JdbcCodec(name, (a, b, _) => get(a, b), (a, b, c, _) => set(a, b, c))

  def withConnection[A](
      name: String,
      get: Using.Manager ?=> (ResultSet, Int, Connection) => Either[String, Option[A]],
      set: Using.Manager ?=> (PreparedStatement, Int, Option[A], Connection) => Unit
  ): JdbcCodec[Option[A]] = JdbcCodec(name, get, set)
}
