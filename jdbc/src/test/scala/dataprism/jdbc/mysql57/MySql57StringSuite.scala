package dataprism.jdbc.mysql57

import dataprism.PlatformStringSuite
import dataprism.jdbc.platform.MySql57JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql57StringSuite extends MySql57FunSuite, PlatformStringSuite[JdbcCodec, MySql57JdbcPlatform] {
  override def maxParallelism: Int = 10

  doTestLpad()
  doTestRpad()

  doTestTrimLeading()
  doTestTrimTrailing()

  doRegexMatchesTest()

  doTestLeft()
  doTestRight()

  doTestMd5()
  doTestSha256()

  doTestRepeat()
  doTestReverse()

  doTestHex()
}
