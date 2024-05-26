package dataprism.jdbc.platform

import dataprism.platform.sql.implementations.MySql8Platform

trait MySql8JdbcPlatform  extends MySqlJdbcPlatform, MySql8Platform
object MySql8JdbcPlatform extends MySql8JdbcPlatform {
  override type Api = MySqlApi
  object Api extends MySqlApi

  override type Impl = DefaultCompleteImpl
  object Impl extends DefaultCompleteImpl
}
