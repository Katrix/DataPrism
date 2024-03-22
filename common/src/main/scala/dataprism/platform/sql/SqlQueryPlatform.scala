package dataprism.platform.sql

//noinspection SqlNoDataSourceInspection,ScalaUnusedSymbol
trait SqlQueryPlatform
    extends SqlQueryPlatformBase,
      SqlQueryPlatformDbValue,
      SqlQueryPlatformValueSource,
      SqlQueryPlatformQuery,
      SqlQueryPlatformOperation
