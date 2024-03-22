package dataprism.skunk.testsuites

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite
import dataprism.PlatformFunSuite.DbToTest
import dataprism.skunk.DescriptiveSkunkDb
import dataprism.skunk.platform.PostgresSkunkPlatform
import org.testcontainers.containers.PostgreSQLContainer
import skunk.{Codec, Session}
import natchez.Trace.Implicits.noop

abstract class PostgresFunSuite extends PlatformFunSuite[Codec, PostgresSkunkPlatform](PostgresSkunkPlatform) {

  override def maxParallelism: Int = 10

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
        Session
          .pooled[F](
            host = container.getHost,
            port = container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
            user = container.getUsername,
            password = Some(container.getPassword).filter(_.nonEmpty),
            database = container.getDatabaseName,
            max = 100
          )
          .map(s => DescriptiveSkunkDb[IO](s))
      }
}
