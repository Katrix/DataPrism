package dataprism.jdbc.sqlite

import cats.effect.IO
import dataprism.PlatformOperationSuite
import dataprism.jdbc.platform.SqliteJdbcPlatform
import dataprism.jdbc.sql.JdbcCodec

class SqliteOperationSuite extends SqliteFunSuite with PlatformOperationSuite[IO, JdbcCodec, SqliteJdbcPlatform] {
  doTestDeleteReturning()

  doTestInsertReturning()
  doTestInsertOnConflict()
  doTestInsertOnConflictReturning()

  doTestUpdateFrom()
  doTestUpdateReturning(identity)
  doTestUpdateFromReturning(_.b, Seq("next", "next"))
}
