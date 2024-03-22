package dataprism.skunk.testsuites

import dataprism.PlatformOperationSuite
import dataprism.skunk.platform.PostgresSkunkPlatform
import skunk.Codec

object PostgresOperationSuite extends PostgresFunSuite with PlatformOperationSuite[Codec, PostgresSkunkPlatform] {
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
