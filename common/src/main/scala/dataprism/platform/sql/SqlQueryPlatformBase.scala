package dataprism.platform.sql

import scala.annotation.targetName
import scala.util.NotGiven

import cats.data.State
import dataprism.platform.{MapRes, QueryPlatform}
import dataprism.sharedast.AstRenderer
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatformBase extends QueryPlatform { platform =>

  type Codec[_]
  type Type[A] = SelectedType[Codec, A]
  val AnsiTypes: AnsiTypes[Codec]

  extension [A](tpe: Codec[A]) @targetName("codecTypeName") def name: String

  extension [A](tpe: Type[A]) @targetName("typeName") def name: String = tpe.codec.name

  type Impl <: SqlBaseImpl
  protected val Impl: Impl
  trait SqlBaseImpl

  lazy val sqlRenderer: AstRenderer[Codec]

  trait SqlTaggedState {
    def queryNum: Int

    def columnNum: Int

    def withNewQueryNum(newQueryNum: Int): TaggedState

    def withNewColumnNum(newColumnNum: Int): TaggedState
  }
  type TaggedState <: SqlTaggedState
  protected def freshTaggedState: TaggedState

  type TagState[A] = State[TaggedState, A]

  extension [A](tpe: Type[A])
    @targetName("typeNotNullChoice") def notNullChoice(
        using NotGiven[SqlNull <:< A]
    ): NullabilityTypeChoice[Codec, A, tpe.Dimension] =
      tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A, tpe.Dimension]]

  extension [A](tpe: Type[A])
    @targetName("typeNullableChoice") def nullableChoice(
        using SqlNull <:< A
    ): NullabilityTypeChoice[Codec, A, tpe.Dimension] =
      tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A, tpe.Dimension]]

  extension [A](tpe: Type[A])
    @targetName("typeNullable") def nullable: Type[A | SqlNull] = tpe.choice.nullable.asInstanceOf[Type[A | SqlNull]]
}
