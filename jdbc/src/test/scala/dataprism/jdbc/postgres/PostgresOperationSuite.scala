package dataprism.jdbc.postgres

import cats.effect.IO
import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.PostgresJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class PostgresOperationSuite extends PostgresFunSuite with PlatformOperationSuite[IO, JdbcCodec, PostgresJdbcPlatform] {
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
