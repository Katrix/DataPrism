package dataprism.jdbc.h2

import java.nio.file.{Files, Paths}

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.h2.jdbcx.JdbcDataSource

abstract class H2FunSuite extends PlatformFunSuite[JdbcCodec, H2JdbcPlatform](H2JdbcPlatform) { self =>

  def dbToTest: DbToTest = DbToTest.H2

  override def sharedResource: Resource[IO, DbType] = Resource.make(IO {
    val ds = JdbcDataSource()
    ds.setUrl(s"jdbc:h2:./${self.getClass.getSimpleName}")
    ds.setUser("sa")
    ds.setPassword("")
    CatsDataSourceDb[IO](ds)
  }) { _ =>
    IO.blocking(Files.deleteIfExists(Paths.get(s"./${self.getClass.getSimpleName}.mv.db"))) *>
      IO.blocking(Files.deleteIfExists(Paths.get(s"./${self.getClass.getSimpleName}.trace.db")))
  }
}
