package dataprism.jdbc.mysql57

import cats.effect.{IO, Resource}
import com.mysql.cj.jdbc.MysqlDataSource
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.testcontainers.containers.MySQLContainer

abstract class MySql57FunSuite extends PlatformFunSuite[JdbcCodec, MySql57JdbcPlatform](MySql57JdbcPlatform) {

  def dbToTest: DbToTest = DbToTest.MySql57

  override def sharedResource: Resource[IO, DbType] =
    Resource
      .make(IO.blocking {
        val container = new MySQLContainer("mysql:5.7.44")
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
