package dataprism

import cats.effect.{IO, Resource}
import dataprism.PlatformFunSuite.DbToTest
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.Db
import weaver.scalacheck.Checkers
import weaver.{Expectations, IOSuite, Log, TestName}

trait PlatformFunSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }](
    val platform: Platform
) extends IOSuite,
      Checkers:

  def dbToTest: DbToTest

  type DbType       = Db[IO, Codec0]
  override type Res = DbType

  def dbTest(name: TestName)(run: DbType ?=> IO[Expectations]): Unit = test(name): db =>
    given DbType = db
    IO(run).flatten

  def dbLogTest(name: TestName)(run: DbType ?=> Log[IO] => IO[Expectations]): Unit = test(name): (db, log) =>
    given DbType = db
    IO(run(log)).flatten

object PlatformFunSuite:
  enum DbToTest:
    case Postgres
    case MySql57
    case MySql8
    case H2
    case Sqlite
