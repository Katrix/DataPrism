package dataprism.jdbc.h2

import dataprism.PlatformStringSuite
import dataprism.jdbc.platform.H2JdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object H2StringSuite extends H2FunSuite, PlatformStringSuite[JdbcCodec, H2JdbcPlatform] {
  override def padSingleCharacterOnly: Boolean = true

  doTestLpad()
  doTestRpad()

  doTestTrimLeading()
  doTestTrimTrailing()

  doTestLeft()
  doTestRight()

  doTestMd5()
  doTestSha256()

  doTestRepeat()
}
