package dataprism.jdbc.postgres

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

abstract class PostgresFunSuite extends PlatformFunSuite[JdbcCodec, PostgresJdbcPlatform](PostgresJdbcPlatform) {

  def dbToTest: DbToTest = DbToTest.Postgres
  
  override def sharedResource: Resource[IO, DbType] =
    Resource
      .make(IO.blocking {
        val container = new PostgreSQLContainer("postgres:16")
        container.start()
        container
      }) { container =>
        IO.blocking(container.stop())
      }
      .flatMap { container =>
        Resource.make(IO {
          val ds = new PGSimpleDataSource()
          ds.setUrl(container.getJdbcUrl)
          ds.setUser(container.getUsername)
          ds.setPassword(container.getPassword)
          ds.setDatabaseName(container.getDatabaseName)

          CatsDataSourceDb[IO](ds)
        })(_ => IO.unit)
      }
}
