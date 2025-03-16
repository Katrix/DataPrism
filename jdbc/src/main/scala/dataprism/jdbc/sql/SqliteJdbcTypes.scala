package dataprism.jdbc.sql

import java.sql.Types

import dataprism.sql.{NullabilityTypeChoice, SqlNull}

trait SqliteJdbcTypes extends JdbcAnsiTypes {
  private def tc[A](codec: JdbcCodec[A | SqlNull]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  val tinyint: TypeOf[Byte] = tc(JdbcCodec.withWasNullCheck("TINYINT", Types.TINYINT, _.getByte(_), _.setByte(_, _)))
}
object SqliteJdbcTypes extends SqliteJdbcTypes
