package dataprism.platform.sql

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends SqlQueryPlatformBase,
      SqlDbValues,
      SqlSimpleMath,
      SqlValueSources,
      SqlQueries,
      SqlOperations
