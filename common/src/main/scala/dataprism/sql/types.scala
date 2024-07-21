package dataprism.sql

import scala.compiletime.ops.int.*
import scala.util.NotGiven

import cats.Invariant
import cats.syntax.all.*

//noinspection ApparentResultTypeRefinement
trait NullabilityTypeChoice[Codec[_], A, Dimension0 <: Int] extends SelectedType[Codec, A]:
  def notNull: SelectedType[Codec, A] { type NNA = A; type Dimension = Dimension0 }
  def nullable: SelectedType[Codec, Option[A]] { type NNA = A; type Dimension = Dimension0 }

  type NNA       = A
  type Dimension = Dimension0

  def codec: Codec[A]                                     = notNull.codec
  val choice: NullabilityTypeChoice[Codec, A, Dimension0] = this

class NullabilityTypeChoiceNoArr[Codec[_], A](
    notNullCodec: Codec[A],
    nullableCodec: Codec[Option[A]]
) extends NullabilityTypeChoice[Codec, A, 0]:
  self =>

  val notNull: NotNullType[Codec, A]   = NotNullType(notNullCodec, this)
  val nullable: NullableType[Codec, A] = NullableType(nullableCodec, this)

  override type Element = Nothing

  override def elementType(using ev: (0 > 0) =:= true): SelectedType[Codec, Nothing] { type Dimension = -1 + 0 } =
    sys.error("impossible")

  def imap[B, NewElement](
      f: A => B
  )(g: B => A)(using NotGiven[B <:< Option[?]], Invariant[Codec]): NullabilityTypeChoiceNoArr[Codec, B] =
    NullabilityTypeChoiceNoArr(
      notNullCodec.imap(f)(g),
      nullableCodec.imap(_.map(f))(_.map(g))
    )

class NullabilityTypeChoiceArr[Codec[_], Arr[_], A, DimensionE <: Int](
    notNullCodec: Codec[Arr[A]],
    nullableCodec: Codec[Option[Arr[A]]],
    element: SelectedType[Codec, A] { type Dimension = DimensionE }
) extends NullabilityTypeChoice[Codec, Arr[A], 1 + DimensionE]:
  self =>

  val notNull: NotNullArrayType[Codec, Arr, A, DimensionE]   = NotNullArrayType(notNullCodec, this, element)
  val nullable: NullableArrayType[Codec, Arr, A, DimensionE] = NullableArrayType(nullableCodec, this, element)

  override type Element = A

  override def elementType(
      using ev: ((1 + DimensionE) > 0) =:= true
  ): SelectedType[Codec, A] { type Dimension = -1 + (1 + DimensionE) } = notNull.elementType

object NullabilityTypeChoice:
  def nullableByDefault[Codec[_], A](
      codec: Codec[Option[A]],
      get: Codec[Option[A]] => Codec[A]
  ): NullabilityTypeChoiceNoArr[Codec, A] =
    NullabilityTypeChoiceNoArr(get(codec), codec)

  def notNullByDefault[Codec[_], A](
      codec: Codec[A],
      nullable: Codec[A] => Codec[Option[A]]
  ): NullabilityTypeChoiceNoArr[Codec, A] =
    NullabilityTypeChoiceNoArr(codec, nullable(codec))

  def nullableByDefaultDimensional[Codec[_], Arr[_], A, ElemDimension <: Int](
      codec: Codec[Option[Arr[A]]],
      get: Codec[Option[Arr[A]]] => Codec[Arr[A]],
      element: SelectedType[Codec, A] {
        type Dimension = ElemDimension
      }
  ): NullabilityTypeChoiceArr[Codec, Arr, A, ElemDimension] =
    NullabilityTypeChoiceArr(get(codec), codec, element)

  def notNullByDefaultDimensional[Codec[_], Arr[_], A, ElemDimension <: Int](
      codec: Codec[Arr[A]],
      nullable: Codec[Arr[A]] => Codec[Option[Arr[A]]],
      element: SelectedType[Codec, A] {
        type Dimension = ElemDimension
      }
  ): NullabilityTypeChoiceArr[Codec, Arr, A, ElemDimension] =
    NullabilityTypeChoiceArr(codec, nullable(codec), element)

sealed trait SelectedType[Codec[_], A]:
  self =>
  type NNA
  type Element
  type Dimension <: Int
  type T = A
  def codec: Codec[A]
  def choice: NullabilityTypeChoice[Codec, NNA, Dimension]

  // noinspection ApparentResultTypeRefinement
  def elementType(using ev: (Dimension > 0) =:= true): SelectedType[Codec, Element] {
    type Dimension = -1 + self.Dimension
  }

  inline def forgetNNA: SelectedType[Codec, A] = this

case class NotNullType[Codec[_], A](
    codec: Codec[A],
    choice: NullabilityTypeChoice[Codec, A, 0]
) extends SelectedType[Codec, A]:
  type NNA                = A
  override type Element   = Nothing
  override type Dimension = 0

  override def elementType(using ev: false =:= true): SelectedType[Codec, Nothing] {
    type Dimension = -1 + 0
  } = sys.error("impossible")

case class NullableType[Codec[_], A](
    codec: Codec[Option[A]],
    choice: NullabilityTypeChoice[Codec, A, 0]
) extends SelectedType[Codec, Option[A]]:
  type NNA                = A
  override type Element   = Nothing
  override type Dimension = 0

  override def elementType(using ev: false =:= true): SelectedType[Codec, Nothing] {
    type Dimension = -1 + 0
  } = sys.error("impossible")

case class NotNullArrayType[Codec[_], Arr[_], A, DimensionE <: Int](
    codec: Codec[Arr[A]],
    choice: NullabilityTypeChoice[Codec, Arr[A], 1 + DimensionE],
    element: SelectedType[Codec, A] {
      type Dimension = DimensionE
    }
) extends SelectedType[Codec, Arr[A]]:
  self =>
  type NNA       = Arr[A]
  type Element   = A
  type Dimension = 1 + DimensionE

  def elementType(
      using ev: 1 + DimensionE > 0 =:= true
  ): SelectedType[Codec, Element] {
    type Dimension = -1 + (1 + DimensionE)
  } = element.asInstanceOf[
    SelectedType[Codec, Element] {
      type Dimension = -1 + (1 + DimensionE)
    }
  ]

case class NullableArrayType[Codec[_], Arr[_], A, DimensionE <: Int](
    codec: Codec[Option[Arr[A]]],
    choice: NullabilityTypeChoice[Codec, Arr[A], 1 + DimensionE],
    element: SelectedType[Codec, A] {
      type Dimension = DimensionE
    }
) extends SelectedType[Codec, Option[Arr[A]]]:
  self =>
  type NNA       = Arr[A]
  type Element   = A
  type Dimension = 1 + DimensionE

  def elementType(
      using ev: 1 + DimensionE > 0 =:= true
  ): SelectedType[Codec, Element] {
    type Dimension = -1 + (1 + DimensionE)
  } = element.asInstanceOf[
    SelectedType[Codec, Element] {
      type Dimension = -1 + (1 + DimensionE)
    }
  ]
