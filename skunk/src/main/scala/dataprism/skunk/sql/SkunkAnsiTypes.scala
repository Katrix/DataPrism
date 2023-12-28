package dataprism.skunk.sql

import java.sql.{Date, Time, Timestamp}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime, ZoneOffset}

import scala.util.NotGiven

import dataprism.sql.AnsiTypes
import skunk.Codec
import skunk.codec.all

object SkunkAnsiTypes extends AnsiTypes[Codec] {

  override def smallint: Codec[Short] = all.int2

  override def integer: Codec[Int] = all.int4

  override def bigint: Codec[Long] = all.int8

  override def real: Codec[Float] = all.float4

  override def doublePrecision: Codec[Double] = all.float8

  override def varchar(n: Int): Codec[String] = all.varchar(n)

  override def defaultStringType: Codec[String] = all.text

  override def date: Codec[Date] = all.date.imap(d => java.sql.Date.valueOf(d))(d => d.toLocalDate)

  override def time: Codec[Time] = all.time.imap(d => java.sql.Time.valueOf(d))(d => d.toLocalTime)

  override def timeWithTimezone: Codec[Time] =
    all.timetz.imap(d => java.sql.Time.valueOf(d.toLocalTime))(d => d.toLocalTime.atOffset(ZoneOffset.UTC))

  override def timestamp: Codec[Timestamp] =
    all.timestamp.imap(t => java.sql.Timestamp.valueOf(t))(t => t.toLocalDateTime)

  override def timestampWithTimezone: Codec[Timestamp] =
    all.timestamptz.imap(t => java.sql.Timestamp.valueOf(t.toLocalDateTime))(t =>
      t.toLocalDateTime.atOffset(ZoneOffset.UTC)
    )

  override def boolean: Codec[Boolean] = all.bool

  override def blob: Codec[Seq[Byte]] = all.bytea.imap(_.toSeq)(_.toArray)

  override def nullable[A](tpe: Codec[A])(using ev: NotGiven[A <:< Option[?]]): Codec[Nullable[A]] =
    tpe.opt.asInstanceOf[Codec[Nullable[A]]]
}
