package dataprism.jdbc.mysql8

import cats.effect.IO
import com.mysql.cj.jdbc.MysqlDataSource
import dataprism.PlatformFunSuite
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.MySqlJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import dataprism.sql.Db
import org.testcontainers.containers.MySQLContainer

class MySql8FunSuite extends PlatformFunSuite[IO, JdbcCodec, MySqlJdbcPlatform](MySqlJdbcPlatform) {

  override val dbFixture: Fixture[Db[IO, JdbcCodec]] = new Fixture[Db[IO, JdbcCodec]]("db") {
    private var container: MySQLContainer[_] = _

    override def apply(): Db[IO, JdbcCodec] = {
      val ds = new MysqlDataSource()
      ds.setUrl(container.getJdbcUrl)
      ds.setUser(container.getUsername)
      ds.setPassword(container.getPassword)
      ds.setDatabaseName(container.getDatabaseName)

      CatsDataSourceDb[IO](ds)
    }

    override def beforeAll(): Unit = {
      container = new MySQLContainer("mysql:8.0.36")
      container.start()
    }

    override def afterAll(): Unit = container.stop()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(dbFixture)
}
