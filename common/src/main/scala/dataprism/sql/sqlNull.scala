package dataprism.sql

import scala.util.NotGiven

import cats.{Applicative, Functor}

sealed trait SqlNull
object SqlNull extends SqlNull
type Nullable[A] = A | SqlNull
object Nullable {
  opaque type NullableSyntax[A] = A | SqlNull
  def syntax[A](v: A | SqlNull)(using NotGiven[SqlNull <:< A]): NullableSyntax[A & v.type] = v
  def unsafeSyntax[A](v: A | SqlNull): NullableSyntax[A] = v

  extension [A](v: NullableSyntax[A]) {
    def unsafeGet: A =
      if v == SqlNull then throw new NoSuchElementException("SqlNull.unsafeGet") else v.asInstanceOf[A]
      
    def orSqlNull: A | SqlNull = v

    def orNull: A | Null = v match
      case SqlNull => null
      case _       => v.unsafeGet

    def toOption: Option[A] = v match
      case SqlNull => None
      case _       => Some(v.unsafeGet)

    def map[B](f: A => B)(using NotGiven[SqlNull <:< B]): NullableSyntax[B] = v match
      case SqlNull => SqlNull
      case _       => f(v.unsafeGet)

    def flatMap[B](f: A => NullableSyntax[B])(using NotGiven[SqlNull <:< B]): NullableSyntax[B] = v match
      case SqlNull => SqlNull
      case _       => f(v.unsafeGet)

    def fold[B](ifNull: => B)(f: A => B): B = v match
      case SqlNull => ifNull
      case _       => f(v.unsafeGet)

    def orElse[B >: A](default: NullableSyntax[B])(using NotGiven[SqlNull <:< A]): NullableSyntax[B] = v match
      case SqlNull => default
      case _       => v.unsafeGet

    def getOrElse[B >: A](default: B): B = v match
      case SqlNull => default
      case _       => v.unsafeGet

    def contains[B >: A](elem: B): Boolean = v match
      case SqlNull => false
      case _       => v.unsafeGet == elem

    def map2[B, C](that: NullableSyntax[B])(f: (A, B) => C): NullableSyntax[C] = (v, that) match
      case (SqlNull, _) => SqlNull
      case (_, SqlNull) => SqlNull
      case (_, _)       => f(v.unsafeGet, that.unsafeGet)
  }

  given Functor[NullableSyntax] with Applicative[NullableSyntax] with {
    override def pure[A](x: A): NullableSyntax[A] = x

    override def ap[A, B](ff: NullableSyntax[A => B])(fa: NullableSyntax[A]): NullableSyntax[B] =
      ff.flatMap(f => fa.map(a => f(a)))
  }
}
