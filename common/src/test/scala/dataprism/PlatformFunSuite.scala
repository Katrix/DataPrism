package dataprism

import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.Db
import munit.{FunSuite, ScalaCheckSuite}

trait PlatformFunSuite[F[_], Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }](val platform: Platform)
    extends FunSuite,
      ScalaCheckSuite:

  type DbType = Db[F, Codec0]
  def dbFixture: Fixture[Db[F, Codec0]]
