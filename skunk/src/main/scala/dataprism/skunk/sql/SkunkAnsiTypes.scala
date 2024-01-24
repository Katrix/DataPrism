package dataprism.skunk.sql

import java.sql.{Date, Time, Timestamp}
import java.time.ZoneOffset

import dataprism.sql.{AnsiTypes, NullabilityTypeChoice}
import skunk.Codec
import skunk.codec.all

object SkunkAnsiTypes extends AnsiTypes[Codec] {
  extension [A](skunkCodec: Codec[A])
    def wrap: TypeOf[A] =
      if skunkCodec.types.length != 1 then
        throw new IllegalArgumentException("Skunk types must always have only one SQL type")
      NullabilityTypeChoice.notNullByDefault(skunkCodec, _.opt)

  override val smallint: TypeOf[Short] = all.int2.wrap

  override val integer: TypeOf[Int] = all.int4.wrap

  override val bigint: TypeOf[Long] = all.int8.wrap

  override val real: TypeOf[Float] = all.float4.wrap

  override val doublePrecision: TypeOf[Double] = all.float8.wrap

  override def varchar(n: Int): TypeOf[String] = all.varchar(n).wrap

  override val defaultStringType: TypeOf[String] = all.text.wrap

  override val date: TypeOf[Date] = all.date.imap(d => java.sql.Date.valueOf(d))(d => d.toLocalDate).wrap

  override val time: TypeOf[Time] = all.time.imap(d => java.sql.Time.valueOf(d))(d => d.toLocalTime).wrap

  override val timeWithTimezone: TypeOf[Time] =
    all.timetz.imap(d => java.sql.Time.valueOf(d.toLocalTime))(d => d.toLocalTime.atOffset(ZoneOffset.UTC)).wrap

  override val timestamp: TypeOf[Timestamp] =
    all.timestamp.imap(t => java.sql.Timestamp.valueOf(t))(t => t.toLocalDateTime).wrap

  override val timestampWithTimezone: TypeOf[Timestamp] =
    all.timestamptz
      .imap(t => java.sql.Timestamp.valueOf(t.toLocalDateTime))(t => t.toLocalDateTime.atOffset(ZoneOffset.UTC))
      .wrap

  override val boolean: TypeOf[Boolean] = all.bool.wrap

  override val blob: TypeOf[Seq[Byte]] = all.bytea.imap(_.toSeq)(_.toArray).wrap
}
