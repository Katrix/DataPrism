package dataprism.skunk.testsuites

import dataprism.PlatformQuerySuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import skunk.Codec

object PostgresQuerySuite extends PostgresFunSuite, PlatformQuerySuite[Codec, PostgresSkunkPlatform] {
  doTestFlatmapLateral()
  doTestExcept()
  doTestIntersect()
  doTestIntersectAll()
  doTestExceptAll()
}
