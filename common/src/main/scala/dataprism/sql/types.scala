package dataprism.sql

import scala.util.NotGiven

import cats.Invariant
import cats.syntax.all.*

class NullabilityTypeChoice[Codec[_], A](
    notNullCodec: Codec[A],
    nullableCodec: Codec[Option[A]]
) extends SelectedType[Codec, A]:
  val notNull: SelectedType[Codec, A] { type NNA = A } = NotNullType(notNullCodec, this)
  val nullable: SelectedType[Codec, Option[A]] { type NNA = A } =
    NullableType(nullableCodec, this)

  override type NNA = A
  override val codec: Codec[A]                         = notNullCodec
  override val choice: NullabilityTypeChoice[Codec, A] = this

  def imap[B](f: A => B)(
      g: B => A
  )(using NotGiven[B <:< Option[?]], Invariant[Codec]): NullabilityTypeChoice[Codec, B] =
    NullabilityTypeChoice(
      notNullCodec.imap[B](f)(g),
      nullableCodec.imap[Option[B]](na => na.map(f))((b: Option[B]) => b.map(g))
    )

object NullabilityTypeChoice:
  def nullableByDefault[Codec[_], A](
      codec: Codec[Option[A]],
      get: Codec[Option[A]] => Codec[A]
  ): NullabilityTypeChoice[Codec, A] = NullabilityTypeChoice(get(codec), codec)
  def notNullByDefault[Codec[_], A](
      codec: Codec[A],
      nullable: Codec[A] => Codec[Option[A]]
  ): NullabilityTypeChoice[Codec, A] = NullabilityTypeChoice(codec, nullable(codec))

sealed trait SelectedType[Codec[_], A]:
  type NNA
  type T = A
  def codec: Codec[A]
  def choice: NullabilityTypeChoice[Codec, NNA]

  inline def forgetNNA: SelectedType[Codec, A] = this

case class NotNullType[Codec[_], A](codec: Codec[A], choice: NullabilityTypeChoice[Codec, A])
    extends SelectedType[Codec, A]:
  type NNA = A

case class NullableType[Codec[_], A](
    codec: Codec[Option[A]],
    choice: NullabilityTypeChoice[Codec, A]
) extends SelectedType[Codec, Option[A]]:
  type NNA = A
