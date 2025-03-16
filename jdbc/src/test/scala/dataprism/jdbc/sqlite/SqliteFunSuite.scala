package dataprism.jdbc.sqlite

import java.nio.file.{Files, Paths}

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

abstract class SqliteFunSuite extends PlatformFunSuite[JdbcCodec, SqliteJdbcPlatform](SqliteJdbcPlatform) { self =>

  def dbToTest: DbToTest = DbToTest.Sqlite

  override def maxParallelism: Int = 1 // Sqlite doesn't seem to like higher parallelisms

  override def sharedResource: Resource[IO, DbType] = {
    val simpleName  = self.getClass.getSimpleName
    val simplerName = if simpleName.endsWith("$") then simpleName.substring(0, simpleName.length - 1) else simpleName
    val ds          = org.sqlite.SQLiteDataSource()
    ds.setUrl(s"jdbc:sqlite:$simplerName.db")
    // ds.setUrl(s"jdbc:sqlite::memory:") //Doesn't seem to retain tables for the next query
    // Resource.pure(CatsDataSourceDb[IO](ds))
    Resource.make(IO.pure(CatsDataSourceDb[IO](ds)))(_ => IO(Files.delete(Paths.get(s"$simplerName.db"))))
  }
}
