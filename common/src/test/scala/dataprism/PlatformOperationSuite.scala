package dataprism

import dataprism.platform.sql.SqlQueryPlatform
import munit.FunSuite

trait PlatformOperationSuite[F[_], Codec0[_]] extends PlatformFunSuite[F, Codec0] {

}
