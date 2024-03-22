package dataprism.jdbc.platform

import dataprism.platform.implementations.MySql57QueryPlatform

trait MySql57JdbcPlatform  extends MySqlJdbcPlatform, MySql57QueryPlatform
object MySql57JdbcPlatform extends MySql57JdbcPlatform {
  override type Api = MySqlApi
  object Api extends MySqlApi
}
