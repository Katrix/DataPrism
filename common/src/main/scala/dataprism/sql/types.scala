package dataprism.sql

import scala.util.NotGiven

import cats.Invariant
import cats.syntax.all.*

class NullabilityTypeChoice[A, Codec[_]](
    notNullCodec: Codec[A],
    nullableCodec: Codec[NullabilityTypeChoice.Nullable[A]]
):
  val notNull: SelectedType[A, Codec] { type NNA = A } = NotNullType(notNullCodec, this)
  val nullable: SelectedType[NullabilityTypeChoice.Nullable[A], Codec] { type NNA = A } =
    NullableType(nullableCodec, this)

  def imap[B](f: A => B)(
      g: B => A
  )(using NotGiven[B <:< Option[_]], Invariant[Codec]): NullabilityTypeChoice[B, Codec] =
    NullabilityTypeChoice(
      notNullCodec.imap[B](f)(g),
      nullableCodec.imap[NullabilityTypeChoice.Nullable[B]](na =>
        na.asInstanceOf[Option[A]].map(f).asInstanceOf[NullabilityTypeChoice.Nullable[B]]
      )((b: NullabilityTypeChoice.Nullable[B]) =>
        b.asInstanceOf[Option[B]].map(g).asInstanceOf[NullabilityTypeChoice.Nullable[A]]
      )
    )

object NullabilityTypeChoice:
  type Nullable[A] = A match {
    case Option[b] => Option[b]
    case _         => Option[A]
  }

  def nullableByDefault[A, Codec[_]](
      codec: Codec[Nullable[A]],
      get: Codec[Nullable[A]] => Codec[A]
  ): NullabilityTypeChoice[A, Codec] = NullabilityTypeChoice(get(codec), codec)
  def notNullByDefault[A, Codec[_]](
      codec: Codec[A],
      nullable: Codec[A] => Codec[Nullable[A]]
  ): NullabilityTypeChoice[A, Codec] = NullabilityTypeChoice(codec, nullable(codec))

sealed trait SelectedType[A, Codec[_]]:
  type NNA
  def codec: Codec[A]
  def choice: NullabilityTypeChoice[NNA, Codec]

case class NotNullType[A, Codec[_]](codec: Codec[A], choice: NullabilityTypeChoice[A, Codec])
    extends SelectedType[A, Codec]:
  type NNA = A

case class NullableType[A, Codec[_]](
    codec: Codec[NullabilityTypeChoice.Nullable[A]],
    choice: NullabilityTypeChoice[A, Codec]
) extends SelectedType[NullabilityTypeChoice.Nullable[A], Codec]:
  type NNA = A
