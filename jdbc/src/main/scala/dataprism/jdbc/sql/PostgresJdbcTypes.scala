package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

import scala.annotation.unused
import scala.util.NotGiven

import dataprism.sql.{NullabilityTypeChoice, SelectedType}

trait PostgresJdbcTypes extends JdbcAnsiTypes:
  private def tc[A](codec: JdbcCodec[Option[A]]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  val text: TypeOf[String] = tc(
    JdbcCodec.withWasNullCheck[String]("TEXT", Types.VARCHAR, _.getString(_), _.setString(_, _))
  )

  override def defaultStringType: TypeOf[String] = text

  val uuid: TypeOf[UUID] = tc(JdbcCodec.byClass[UUID]("UUID", 2950))

  object javaTime:
    val date: TypeOf[LocalDate] = tc(JdbcCodec.byClass[LocalDate]("DATE", Types.DATE))
    val time: TypeOf[LocalTime] = tc(JdbcCodec.byClass[LocalTime]("TIME", Types.TIME))
    val timeWithTimezone: TypeOf[OffsetTime] =
      tc(JdbcCodec.byClass[OffsetTime]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE))

    val timestamp: TypeOf[LocalDateTime] = tc(JdbcCodec.byClass[LocalDateTime]("TIMESTAMP", Types.TIMESTAMP))

    val timestampWithTimezone: TypeOf[OffsetDateTime] =
      tc(JdbcCodec.byClass[OffsetDateTime]("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE))

object PostgresJdbcTypes extends PostgresJdbcTypes
