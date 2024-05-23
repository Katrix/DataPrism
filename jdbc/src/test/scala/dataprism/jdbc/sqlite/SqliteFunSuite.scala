package dataprism.jdbc.sqlite

import java.nio.file.{Files, Paths}

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.sqlite.SQLiteDataSource

abstract class SqliteFunSuite extends PlatformFunSuite[JdbcCodec, SqliteJdbcPlatform](SqliteJdbcPlatform) { self =>

  def dbToTest: DbToTest = DbToTest.Sqlite

  override def sharedResource: Resource[IO, DbType] = {
    val simpleName = self.getClass.getSimpleName
    val simplerName = if simpleName.endsWith("$") then simpleName.substring(0, simpleName.length - 1) else simpleName
    Resource.make(IO {
      val ds         = org.sqlite.SQLiteDataSource()
      ds.setUrl(s"jdbc:sqlite:$simplerName.db")
      CatsDataSourceDb[IO](ds)
    }) { _ =>
      IO.blocking(Files.deleteIfExists(Paths.get(s"./$simplerName.db")))
    }
  }
}
