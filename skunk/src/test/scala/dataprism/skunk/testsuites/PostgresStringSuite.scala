package dataprism.skunk.testsuites

import dataprism.PlatformStringSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import skunk.Codec

object PostgresStringSuite extends PostgresFunSuite, PlatformStringSuite[Codec, PostgresSkunkPlatform] {
  override def maxParallelism: Int = 10

  doTestLpad()
  doTestRpad()

  doTestTrimLeading()
  doTestTrimTrailing()

  doRegexMatchesTest()

  doTestLeft()
  doTestRight()

  doTestMd5()

  doTestRepeat()
  doTestReverse()

  doTestHex()
}
