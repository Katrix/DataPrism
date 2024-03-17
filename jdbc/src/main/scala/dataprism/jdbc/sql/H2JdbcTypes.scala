package dataprism.jdbc.sql

import dataprism.sql.NullabilityTypeChoice

import java.sql.Types
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

trait H2JdbcTypes extends JdbcAnsiTypes:
  private def tc[A](codec: JdbcCodec[Option[A]]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  val uuid: TypeOf[UUID] = tc(JdbcCodec.byClass[UUID]("UUID", 2950))

  object javaTime:
    val date: TypeOf[LocalDate] = tc(JdbcCodec.byClass[LocalDate]("DATE", Types.DATE))
    val time: TypeOf[LocalTime] = tc(JdbcCodec.byClass[LocalTime]("TIME", Types.TIME))
    val timeWithTimezone: TypeOf[OffsetTime] =
      tc(JdbcCodec.byClass[OffsetTime]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE))

    val timestamp: TypeOf[LocalDateTime] = tc(JdbcCodec.byClass[LocalDateTime]("TIMESTAMP", Types.TIMESTAMP))

    val timestampWithTimezone: TypeOf[OffsetDateTime] =
      tc(JdbcCodec.byClass[OffsetDateTime]("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE))


object H2JdbcTypes extends JdbcAnsiTypes
