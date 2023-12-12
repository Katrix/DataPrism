package dataprism.skunk.sql

import dataprism.sql.AnsiTypes

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import scala.util.NotGiven
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

  override def date: Codec[LocalDate] = all.date

  override def time: Codec[LocalTime] = all.time

  override def timeWithTimezone: Codec[OffsetTime] = all.timetz

  override def timestamp: Codec[LocalDateTime] = all.timestamp

  override def timestampWithTimezone: Codec[OffsetDateTime] = all.timestamptz

  override def boolean: Codec[Boolean] = all.bool

  override def blob: Codec[Seq[Byte]] = all.bytea.imap(_.toSeq)(_.toArray)

  override def nullable[A](tpe: Codec[A])(using ev: NotGiven[A <:< Option[?]]): Codec[Nullable[A]] =
    tpe.opt.asInstanceOf[Codec[Nullable[A]]]
}
