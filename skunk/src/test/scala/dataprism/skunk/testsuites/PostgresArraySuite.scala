package dataprism.skunk.testsuites

import dataprism.PlatformArraysSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import org.scalacheck.Gen
import skunk.Codec

object PostgresArraySuite extends PostgresFunSuite, PlatformArraysSuite[Codec, PostgresSkunkPlatform] {
  override def maxParallelism: Int = 10
  import platform.Api.{*, given}

  testArrays(platform.AnsiTypes.integer, Gen.choose(-10000, 10000))
  testArrayUnnest(platform.AnsiTypes.integer, Gen.choose(-10000, 10000), _ + 1, _ + 1.as(platform.AnsiTypes.integer))
  testArrays(platform.AnsiTypes.defaultStringType, Gen.asciiPrintableStr)
  testArrayUnnest(
    platform.AnsiTypes.defaultStringType,
    Gen.asciiPrintableStr,
    _ ++ "foo",
    _ ++ "foo".as(platform.AnsiTypes.defaultStringType)
  )
}
