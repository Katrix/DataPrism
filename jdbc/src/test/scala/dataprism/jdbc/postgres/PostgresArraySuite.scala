package dataprism.jdbc.postgres

import dataprism.PlatformArraysSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.scalacheck.Gen

object PostgresArraySuite extends PostgresFunSuite, PlatformArraysSuite[JdbcCodec, PostgresJdbcPlatform] {
  override def maxParallelism: Int = 10
  import platform.Api.{*, given}

  testArrays(platform.AnsiTypes.integer, Gen.choose(-10000, 10000))
  testArrayUnnest(platform.AnsiTypes.integer, Gen.choose(-10000, 10000), _ + 1, _ + 1.as(platform.AnsiTypes.integer))
}
