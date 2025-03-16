package dataprism.sql

import scala.util.NotGiven

import cats.{Applicative, Functor}

sealed trait SqlNull
object SqlNull extends SqlNull
type Nullable[A] = A | SqlNull

object sqlNullSyntax {
  extension [A](v: A | SqlNull)(using NotGiven[SqlNull <:< A])
    def unsafeGet: A & v.type =
      if v == SqlNull then throw new NoSuchElementException("SqlNull.unsafeGet") else v.asInstanceOf[A & v.type]

    def orNull: (A & v.type) | Null = v match
      case SqlNull => null
      case _       => v.unsafeGet

    def toOption: Option[A & v.type] = v match
      case SqlNull => None
      case _       => Some(v.unsafeGet)

    def map[B](f: (A & v.type) => B): B | SqlNull = v match
      case SqlNull => SqlNull
      case _       => f(v.unsafeGet)

    def flatMap[B](f: (A & v.type) => B | SqlNull): B | SqlNull = v match
      case SqlNull => SqlNull
      case _       => f(v.unsafeGet)

    def fold[B](ifNull: => B)(f: (A & v.type) => B): B = v match
      case SqlNull => ifNull
      case _       => f(v.unsafeGet)

    def orElse[B >: A](default: B | SqlNull): B | SqlNull = v match
      case SqlNull => default
      case _       => v.unsafeGet

    def getOrElse[B >: A](default: B): B = v match
      case SqlNull => default
      case _       => v.unsafeGet

    def contains[B >: A](elem: B): Boolean = v match
      case SqlNull => false
      case _       => v.unsafeGet == elem

    def map2[B, C](that: B | SqlNull)(f: (A, B) => C): C | SqlNull = (v, that) match
      case (SqlNull, _) => SqlNull
      case (_, SqlNull) => SqlNull
      case (_, _)       => f(v.unsafeGet, that.unsafeGet)

  given Functor[Nullable] with Applicative[Nullable] with {
    override def pure[A](x: A): Nullable[A] = x

    override def ap[A, B](ff: Nullable[A => B])(fa: Nullable[A]): Nullable[B] =
      ff.flatMap(f => fa.map(a => f(a)))
  }
}
