package dataprism

import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.Db
import munit.{CatsEffectSuite, ScalaCheckEffectSuite}

trait PlatformFunSuite[F[_], Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }](
    val platform: Platform
) extends CatsEffectSuite,
      ScalaCheckEffectSuite:

  type DbType = Db[F, Codec0]
  def dbFixture: Fixture[Db[F, Codec0]]
  
  def clueOp[A, Op <: platform.Operation[A]](op: Op): Op =
    clue(op.sqlAndTypes._1.str)
    op
