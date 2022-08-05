package dataprism.platform.sql

import java.util.UUID

import scala.annotation.targetName

import cats.Applicative
import cats.data.State
import cats.syntax.all.*
import dataprism.platform.base.QueryPlatform
import dataprism.sharedast.SelectAst.Data
import dataprism.sharedast.{AstRenderer, SelectAst, SqlExpr}
import dataprism.sql.{Column, Table}
import perspective.*

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends QueryPlatform
    with SqlQueryPlatformDbValue
    with SqlQueryPlatformValueSource
    with SqlQueryPlatformQuery { platform =>

  val sqlRenderer: AstRenderer

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
