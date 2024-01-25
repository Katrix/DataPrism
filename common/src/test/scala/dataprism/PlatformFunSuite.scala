package dataprism

import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.Db
import munit.{FunSuite, ScalaCheckSuite}

trait PlatformFunSuite[F[_], Codec0[_]](val platform: SqlQueryPlatform { type Codec[A] = Codec0[A] })
    extends FunSuite,
      ScalaCheckSuite:

  type DbType = Db[F, Codec0]
  def dbFixture: Fixture[Db[F, Codec0]]
