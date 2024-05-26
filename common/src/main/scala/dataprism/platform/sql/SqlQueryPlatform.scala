package dataprism.platform.sql

import dataprism.platform.sql.query.{SqlQueries, SqlValueSources}
import dataprism.platform.sql.value.{SqlDbValues, SqlSimpleMath}

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends SqlQueryPlatformBase,
      SqlDbValues,
      SqlSimpleMath,
      SqlValueSources,
      SqlQueries,
      SqlOperations
