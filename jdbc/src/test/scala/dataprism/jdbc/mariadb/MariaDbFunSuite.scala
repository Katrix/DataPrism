package dataprism.jdbc.mariadb

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.mariadb.jdbc.MariaDbPoolDataSource
import org.testcontainers.containers.MariaDBContainer

abstract class MariaDbFunSuite extends PlatformFunSuite[JdbcCodec, MariaDbJdbcPlatform](MariaDbJdbcPlatform) {

  def dbToTest: DbToTest = DbToTest.MySql8

  override def sharedResource: Resource[IO, DbType] =
    Resource
      .make(IO.blocking {
        val container = new MariaDBContainer("mariadb:11.3.2")
        container.start()
        container
      }) { container =>
        IO.blocking(container.stop())
      }
      .flatMap { container =>
        Resource
          .make(IO {
            val ds = new MariaDbPoolDataSource()
            ds.setUrl(container.getJdbcUrl)
            ds.setUser(container.getUsername)
            ds.setPassword(container.getPassword)

            ds
          })(ds => IO.blocking(ds.close()))
          .map(ds => CatsDataSourceDb[IO](ds))
      }
}
