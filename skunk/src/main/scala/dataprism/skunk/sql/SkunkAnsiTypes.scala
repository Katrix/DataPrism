package dataprism.skunk.sql

import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime, ZoneOffset}

import scala.util.NotGiven

import dataprism.sql.{AnsiTypes, NullabilityTypeChoice}
import skunk.Codec
import skunk.codec.all

object SkunkAnsiTypes extends AnsiTypes[Codec] {
  extension [A](skunkCodec: Codec[A])
    def wrap: TypeOf[A] =
      NullabilityTypeChoice.notNullByDefault(skunkCodec, _.opt)

  override def smallint: TypeOf[Short] = all.int2.wrap

  override def integer: TypeOf[Int] = all.int4.wrap

  override def bigint: TypeOf[Long] = all.int8.wrap

  override def real: TypeOf[Float] = all.float4.wrap

  override def doublePrecision: TypeOf[Double] = all.float8.wrap

  override def varchar(n: Int): TypeOf[String] = all.varchar(n).wrap

  override def defaultStringType: TypeOf[String] = all.text.wrap

  override def date: TypeOf[Date] = all.date.imap(d => java.sql.Date.valueOf(d))(d => d.toLocalDate).wrap

  override def time: TypeOf[Time] = all.time.imap(d => java.sql.Time.valueOf(d))(d => d.toLocalTime).wrap

  override def timeWithTimezone: TypeOf[Time] =
    all.timetz.imap(d => java.sql.Time.valueOf(d.toLocalTime))(d => d.toLocalTime.atOffset(ZoneOffset.UTC)).wrap

  override def timestamp: TypeOf[Timestamp] =
    all.timestamp.imap(t => java.sql.Timestamp.valueOf(t))(t => t.toLocalDateTime).wrap

  override def timestampWithTimezone: TypeOf[Timestamp] =
    all.timestamptz
      .imap(t => java.sql.Timestamp.valueOf(t.toLocalDateTime))(t => t.toLocalDateTime.atOffset(ZoneOffset.UTC))
      .wrap

  override def boolean: TypeOf[Boolean] = all.bool.wrap

  override def blob: TypeOf[Seq[Byte]] = all.bytea.imap(_.toSeq)(_.toArray).wrap
}
