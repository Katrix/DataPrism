package dataprism.platform.sql

import dataprism.platform.sql.query.{SqlQueries, SqlValueSources}
import dataprism.platform.sql.value.{SqlDbValues, SqlSimpleMath, SqlStringOps}

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends SqlQueryPlatformBase,
      SqlDbValues,
      SqlSimpleMath,
      SqlStringOps,
      SqlValueSources,
      SqlQueries,
      SqlOperations {

  //For better types in IntelliJ
  type Api <: SqlQueryApi & SqlDbValueApi & SqlDbValueImplApi & SqlStringApi & SqlQueryApi & SqlOperationApi & QueryApi
}
