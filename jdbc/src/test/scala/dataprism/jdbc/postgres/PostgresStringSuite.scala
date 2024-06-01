package dataprism.jdbc.postgres

import dataprism.PlatformStringSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object PostgresStringSuite extends PostgresFunSuite, PlatformStringSuite[JdbcCodec, PostgresJdbcPlatform] {
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
