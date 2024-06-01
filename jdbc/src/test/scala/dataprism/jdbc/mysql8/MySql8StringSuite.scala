package dataprism.jdbc.mysql8

import dataprism.PlatformStringSuite
import dataprism.jdbc.platform.MySql8JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MySql8StringSuite extends MySql8FunSuite, PlatformStringSuite[JdbcCodec, MySql8JdbcPlatform] {
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
