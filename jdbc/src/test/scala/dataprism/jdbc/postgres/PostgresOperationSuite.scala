package dataprism.jdbc.postgres

import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

object PostgresOperationSuite extends PostgresFunSuite with PlatformOperationSuite[JdbcCodec, PostgresJdbcPlatform] {
  doTestDeleteUsing()
  doTestDeleteReturning()
  doTestDeleteUsingReturning()

  doTestInsertReturning()
  doTestInsertOnConflict()
  doTestInsertOnConflictReturning()

  doTestUpdateFrom()
  doTestUpdateReturning((a, _) => a)
  doTestUpdateFromReturning((_, b) => b, Seq(1, 2))
}
