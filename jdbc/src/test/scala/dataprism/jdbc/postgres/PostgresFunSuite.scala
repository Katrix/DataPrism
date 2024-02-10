package dataprism.jdbc.postgres

import cats.effect.IO
import dataprism.PlatformFunSuite
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import dataprism.sql.Db
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.containers.PostgreSQLContainer

class PostgresFunSuite extends PlatformFunSuite[IO, JdbcCodec, PostgresJdbcPlatform](PostgresJdbcPlatform) {

  override val dbFixture: Fixture[Db[IO, JdbcCodec]] = new Fixture[Db[IO, JdbcCodec]]("Db") {
    private var container: PostgreSQLContainer[_] = _

    override def apply(): Db[IO, JdbcCodec] = {
      val ds = new PGSimpleDataSource()
      ds.setUrl(container.getJdbcUrl)
      ds.setUser(container.getUsername)
      ds.setPassword(container.getPassword)
      ds.setDatabaseName(container.getDatabaseName)

      CatsDataSourceDb[IO](ds)
    }

    override def beforeAll(): Unit = {
      container = new PostgreSQLContainer("postgres:16")
      container.start()
    }

    override def afterAll(): Unit = container.stop()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(dbFixture)
}
