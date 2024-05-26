package dataprism.jdbc.platform

import dataprism.platform.sql.implementations.MySql57Platform

trait MySql57JdbcPlatform  extends MySqlJdbcPlatform, MySql57Platform
object MySql57JdbcPlatform extends MySql57JdbcPlatform {
  override type Api = MySqlApi
  object Api extends MySqlApi

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
