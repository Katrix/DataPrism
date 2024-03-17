package dataprism.jdbc.h2

import java.nio.file.{Files, Paths}

import cats.effect.IO
import dataprism.PlatformFunSuite
import dataprism.jdbc.CatsDataSourceDb
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import dataprism.sql.Db
import org.h2.jdbcx.JdbcDataSource
import org.scalacheck.Test

class H2FunSuite extends PlatformFunSuite[IO, JdbcCodec, H2JdbcPlatform](H2JdbcPlatform) { self =>

  override protected def scalaCheckTestParameters: Test.Parameters =
    org.scalacheck.Test.Parameters.default.withMinSuccessfulTests(30)

  override val dbFixture: Fixture[Db[IO, JdbcCodec]] = new Fixture[Db[IO, JdbcCodec]]("Db") {
    private var ds: JdbcDataSource = _

    override def apply(): Db[IO, JdbcCodec] =
      CatsDataSourceDb[IO](ds)

    override def beforeAll(): Unit = {
      ds = JdbcDataSource()
      ds.setUrl(s"jdbc:h2:./${self.getClass.getSimpleName}")
      ds.setUser("sa")
      ds.setPassword("")
    }

    override def afterAll(): Unit =
      Files.deleteIfExists(Paths.get(s"./${self.getClass.getSimpleName}.mv.db"))
      Files.deleteIfExists(Paths.get(s"./${self.getClass.getSimpleName}.trace.db"))
      ()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(dbFixture)
}
