package dataprism.platform.sql

import cats.data.State
import dataprism.platform.base.{MapRes, QueryPlatform}
import dataprism.sharedast.AstRenderer
import dataprism.sql.*
import perspective.*

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatformBase extends QueryPlatform { platform =>

  type Codec[_]
  type Type[A] = SelectedType[Codec, A]
  val AnsiTypes: AnsiTypes[Codec]

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
}
