package dataprism.jdbc.platform

import dataprism.jdbc.platform
import dataprism.platform.implementations.MariaDbQueryPlatform

trait MariaDbJdbcPlatform  extends MySqlJdbcPlatform, MariaDbQueryPlatform
object MariaDbJdbcPlatform extends MariaDbJdbcPlatform {
  override type Api = MySqlApi
  object Api extends MySqlApi

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
