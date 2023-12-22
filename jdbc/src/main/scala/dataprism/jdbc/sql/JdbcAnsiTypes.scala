package dataprism.jdbc.sql

import dataprism.sql.AnsiTypes

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import scala.annotation.unused
import scala.util.NotGiven

trait JdbcAnsiTypes extends AnsiTypes[JdbcType]:
  val smallint: JdbcType[Short]         = JdbcType.simple("SMALLINT", _.getShort(_), _.setShort(_, _))
  val integer: JdbcType[Int]            = JdbcType.simple("INTEGER", _.getInt(_), _.setInt(_, _))
  val bigint: JdbcType[Long]            = JdbcType.simple("BIGINT", _.getLong(_), _.setLong(_, _))
  val real: JdbcType[Float]             = JdbcType.simple("REAL", _.getFloat(_), _.setFloat(_, _))
  val doublePrecision: JdbcType[Double] = JdbcType.simple("DOUBLE PRECISION", _.getDouble(_), _.setDouble(_, _))
  val boolean: JdbcType[Boolean]        = JdbcType.simple("BOOLEAN", _.getBoolean(_), _.setBoolean(_, _))

  def varchar(n: Int): JdbcType[String] = JdbcType.simple(s"VARCHAR($n)", _.getString(_), _.setString(_, _))

  override def defaultStringType: JdbcType[String] = varchar(254)

  val date: JdbcType[LocalDate] = JdbcType.simple(
    "DATE",
    _.getObject(_, classOf[LocalDate]),
    (a, b, c) => a.setObject(b, c)
  )

  val time: JdbcType[LocalTime] = JdbcType.simple(
    "TIME",
    _.getObject(_, classOf[LocalTime]),
    (a, b, c) => a.setObject(b, c)
  )

  val timeWithTimezone: JdbcType[OffsetTime] = JdbcType.simple(
    "TIME WITH TIMEZONE",
    _.getObject(_, classOf[OffsetTime]),
    (a, b, c) => a.setObject(b, c)
  )

  val timestamp: JdbcType[LocalDateTime] = JdbcType.simple(
    "TIMESTAMP",
    _.getObject(_, classOf[LocalDateTime]),
    (a, b, c) => a.setObject(b, c)
  )

  val timestampWithTimezone: JdbcType[OffsetDateTime] =
    JdbcType.simple(
      "TIMESTAMP WITH TIMEZONE",
      _.getObject(_, classOf[OffsetDateTime]),
      (a, b, c) => a.setObject(b, c)
    )

  trait ArrayMapping[A]:
    def makeArrayType(inner: JdbcType[A]): JdbcType[Seq[A]]

  def ArrayMapping: ArrayMappingCompanion
  trait ArrayMappingCompanion:
    given ArrayMapping[Byte] with
      override def makeArrayType(inner: JdbcType[Byte]): JdbcType[Seq[Byte]] =
        JdbcType.simple("BLOB", _.getBytes(_).toSeq, (a, b, c) => a.setBytes(b, c.toArray))

  val blob: JdbcType[Seq[Byte]] = ArrayMapping.given_ArrayMapping_Byte.makeArrayType(
    JdbcType.simple("BYTE", _.getByte(_), (a, b, c) => a.setByte(b, c))
  )

  def array[A](inner: JdbcType[A])(using mapping: ArrayMapping[A]): JdbcType[Seq[A]] =
    mapping.makeArrayType(inner)

  def nullable[A](inner: JdbcType[A])(using @unused ev: NotGiven[A <:< Option[_]]): JdbcType[Nullable[A]] =
    if inner.isNullable then inner.asInstanceOf[JdbcType[Nullable[A]]]
    else
      JdbcType
        .withConnection(
          inner.name,
          (a, b, c) => Option(inner.get(a, b, c)),
          (a, b, c, d) => inner.set(a, b, c.map(_.asInstanceOf[AnyRef]).orNull.asInstanceOf[A], d),
          isNullable = true
        )
        .asInstanceOf[JdbcType[Nullable[A]]]

object JdbcAnsiTypes extends JdbcAnsiTypes:
  override def ArrayMapping: ArrayMappingCompanion = new ArrayMappingCompanion {}
