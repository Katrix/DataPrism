package dataprism.jdbc.sqlite

import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object SqliteOperationSuite extends SqliteFunSuite with PlatformOperationSuite[JdbcCodec, SqliteJdbcPlatform] {
  doTestDeleteReturning()

  doTestInsertReturning()
  doTestInsertOnConflict()
  doTestInsertOnConflictReturning()

  doTestUpdateFrom()
  doTestUpdateReturning(identity)
  doTestUpdateFromReturning(_.b, Seq("next", "next"))
}
