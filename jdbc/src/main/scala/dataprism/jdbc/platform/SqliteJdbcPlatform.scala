package dataprism.jdbc.platform

import scala.annotation.targetName

import dataprism.jdbc.sql.{JdbcCodec, SqliteJdbcTypes}
import dataprism.platform.implementations.SqliteQueryPlatform
import dataprism.sql.AnsiTypes

trait SqliteJdbcPlatform extends SqliteQueryPlatform {

  type Api = SqliteApi
  val Api: Api = new SqliteApi {}

  override type Codec[A] = JdbcCodec[A]
  extension [A](tpe: Codec[A])
    @targetName("codecTypeName")
    override def name: String = tpe.name

  override val AnsiTypes: AnsiTypes[JdbcCodec] = SqliteJdbcTypes

  type Compile = SqlCompile
  object Compile extends SqlCompile
}
object SqliteJdbcPlatform extends SqliteJdbcPlatform
