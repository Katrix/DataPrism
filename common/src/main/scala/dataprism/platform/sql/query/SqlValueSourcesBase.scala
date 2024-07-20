package dataprism.platform.sql.query

import dataprism.platform.sql.SqlQueryPlatformBase
import dataprism.sharedast.SelectAst
import perspective.*

trait SqlValueSourcesBase extends SqlQueryPlatformBase { platform =>

  case class ValueSourceAstMetaData[A[_[_]]](ast: SelectAst.From[Codec], values: A[DbValue])

  trait SqlValueSourceBase[A[_[_]]] {
    def applyKC: ApplyKC[A]
    given ApplyKC[A] = applyKC

    def fromPartAndValues: TagState[ValueSourceAstMetaData[A]]
  }

  type ValueSource[A[_[_]]] <: SqlValueSourceBase[A]
  trait SqlValueSourceCompanion {
    def getFromQuery[A[_[_]]](query: Query[A]): ValueSource[A]
  }

  type ValueSourceCompanion <: SqlValueSourceCompanion
  val ValueSource: ValueSourceCompanion

}
