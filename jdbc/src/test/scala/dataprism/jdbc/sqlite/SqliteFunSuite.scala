package dataprism.jdbc.sqlite

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

abstract class SqliteFunSuite extends PlatformFunSuite[JdbcCodec, SqliteJdbcPlatform](SqliteJdbcPlatform) { self =>

  def dbToTest: DbToTest = DbToTest.Sqlite

  override def sharedResource: Resource[IO, DbType] = {
    val simpleName  = self.getClass.getSimpleName
    val simplerName = if simpleName.endsWith("$") then simpleName.substring(0, simpleName.length - 1) else simpleName
    val ds          = org.sqlite.SQLiteDataSource()
    ds.setUrl(s"jdbc:sqlite::memory:")
    Resource.pure(CatsDataSourceDb[IO](ds))
  }
}
