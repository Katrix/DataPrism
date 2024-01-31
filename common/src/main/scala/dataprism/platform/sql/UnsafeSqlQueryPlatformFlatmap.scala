package dataprism.platform.sql

trait UnsafeSqlQueryPlatformFlatmap extends SqlQueryPlatform {

  type FlatmappableQuery[A[_[_]]] = Query[A]

  extension [A[_[_]], B[_[_]]](sqlQuery: SqlQuery.SqlQueryFlatMap[A, B])
    override def liftSqlQueryFlatMap: FlatmappableQuery[B] = sqlQuery.liftSqlQuery

  extension [A[_[_]]](query: Query[A])
    override def unsafeLiftQueryToFlatmappable: FlatmappableQuery[A] =
      query
}
