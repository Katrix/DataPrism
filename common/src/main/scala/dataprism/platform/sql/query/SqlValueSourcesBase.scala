package dataprism.platform.sql.query

import scala.annotation.targetName
import cats.data.State
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatformBase
import dataprism.sharedast.{SelectAst, SqlExpr}
import dataprism.sql.*
import perspective.*

trait SqlValueSourcesBase extends SqlQueryPlatformBase { platform =>

  case class ValueSourceAstMetaData[A[_[_]]](ast: SelectAst.From[Codec], values: A[DbValue])

  trait SqlValueSourceBase[A[_[_]]] {
    def applyKC: ApplyKC[A]
    given ApplyKC[A] = applyKC

    def fromPartAndValues: TagState[ValueSourceAstMetaData[A]]
  }

  type ValueSource[A[_[_]]] <: SqlValueSourceBase[A]
  type ValueSourceCompanion
  val ValueSource: ValueSourceCompanion

  extension (c: ValueSourceCompanion)
    @targetName("valueSourceGetFromQuery") def getFromQuery[A[_[_]]](query: Query[A]): ValueSource[A]

}
