package dataprism.jdbc.mysql8

import cats.effect.{IO, Resource}
import com.mysql.cj.jdbc.MysqlDataSource
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.testcontainers.containers.MySQLContainer

abstract class MySql8FunSuite extends PlatformFunSuite[JdbcCodec, MySql8JdbcPlatform](MySql8JdbcPlatform) {

  def dbToTest: DbToTest = DbToTest.MySql8

  override def sharedResource: Resource[IO, DbType] =
    Resource
      .make(IO.blocking {
        val container = new MySQLContainer("mysql:8.0.36")
        container.start()
        container
      }) { container =>
        IO.blocking(container.stop())
      }
      .flatMap { container =>
        Resource.make(IO {
          val ds = new MysqlDataSource()
          ds.setUrl(container.getJdbcUrl)
          ds.setUser(container.getUsername)
          ds.setPassword(container.getPassword)
          ds.setDatabaseName(container.getDatabaseName)

          CatsDataSourceDb[IO](ds)
        })(_ => IO.unit)
      }
}
