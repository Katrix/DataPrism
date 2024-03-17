package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformFunSuite
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import dataprism.sql.Db
import org.sqlite.SQLiteDataSource

import java.nio.file.{Files, Paths}

class SqliteFunSuite extends PlatformFunSuite[IO, JdbcCodec, SqliteJdbcPlatform](SqliteJdbcPlatform) { self =>

  override val dbFixture: Fixture[Db[IO, JdbcCodec]] = new Fixture[Db[IO, JdbcCodec]]("Db") {
    private var ds: SQLiteDataSource = _

    override def apply(): Db[IO, JdbcCodec] =
      CatsDataSourceDb[IO](ds)

    override def beforeAll(): Unit = {
      ds = org.sqlite.SQLiteDataSource()
      ds.setUrl(s"jdbc:sqlite:${self.getClass.getSimpleName}.db")
    }

    override def afterAll(): Unit =
      Files.deleteIfExists(Paths.get(s"./${self.getClass.getSimpleName}.db"))
      ()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(dbFixture)
}
