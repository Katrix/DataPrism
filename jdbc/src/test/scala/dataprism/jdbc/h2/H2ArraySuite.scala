package dataprism.jdbc.h2

import dataprism.PlatformArraysSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec
import org.scalacheck.Gen

object H2ArraySuite extends H2FunSuite, PlatformArraysSuite[JdbcCodec, H2JdbcPlatform] {
  override def maxParallelism: Int = 10
  import platform.Api.{*, given}

  testArrays(platform.AnsiTypes.integer, Gen.choose(-10000, 10000))
  testArrayUnnest(platform.AnsiTypes.integer, Gen.choose(-10000, 10000), _ + 1, _ + 1.as(platform.AnsiTypes.integer))
}
