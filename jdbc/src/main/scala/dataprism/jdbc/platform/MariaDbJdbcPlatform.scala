package dataprism.jdbc.platform

import dataprism.jdbc.platform
import dataprism.platform.sql.implementations.MariaDbPlatform

trait MariaDbJdbcPlatform  extends MySqlJdbcPlatform, MariaDbPlatform
object MariaDbJdbcPlatform extends MariaDbJdbcPlatform {
  override type Api = MySqlApi
  object Api extends MySqlApi

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
