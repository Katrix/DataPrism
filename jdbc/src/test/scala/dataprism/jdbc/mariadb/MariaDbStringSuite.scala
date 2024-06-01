package dataprism.jdbc.mariadb

import dataprism.PlatformStringSuite
import dataprism.jdbc.platform.MariaDbJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object MariaDbStringSuite extends MariaDbFunSuite, PlatformStringSuite[JdbcCodec, MariaDbJdbcPlatform] {
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
