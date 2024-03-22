package dataprism.jdbc.platform

import dataprism.platform.implementations.MySql8QueryPlatform

trait MySql8JdbcPlatform  extends MySqlJdbcPlatform, MySql8QueryPlatform
object MySql8JdbcPlatform extends MySql8JdbcPlatform {
  override type Api = MySqlApi
  object Api extends MySqlApi
}
