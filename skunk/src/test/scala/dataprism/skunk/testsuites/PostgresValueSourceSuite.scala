package dataprism.skunk.testsuites

import dataprism.PlatformValueSourceSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import skunk.Codec

object PostgresValueSourceSuite
    extends PostgresFunSuite
    with PlatformValueSourceSuite[Codec, PostgresSkunkPlatform] {
  doTestFullJoin()
}
