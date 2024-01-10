package dataprism.platform.sql

import cats.data.State
import dataprism.platform.base.{MapRes, QueryPlatform}
import dataprism.sharedast.AstRenderer
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends QueryPlatform
    with SqlQueryPlatformDbValue
    with SqlQueryPlatformValueSource
    with SqlQueryPlatformQuery
    with SqlQueryPlatformOperation { platform =>

  type Codec[_]
  type Type[A] = SelectedType[A, Codec]
  def AnsiTypes: AnsiTypes[Codec]

  val sqlRenderer: AstRenderer[Codec]

  trait SqlTaggedState {
    def queryNum: Int

    def columnNum: Int

    def withNewQueryNum(newQueryNum: Int): TaggedState

    def withNewColumnNum(newColumnNum: Int): TaggedState
  }
  type TaggedState <: SqlTaggedState
  protected def freshTaggedState: TaggedState

  type TagState[A] = State[TaggedState, A]
}
