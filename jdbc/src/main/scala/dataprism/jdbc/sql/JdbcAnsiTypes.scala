package dataprism.jdbc.sql

import java.sql.{Connection, Date, PreparedStatement, ResultSet, Time, Timestamp, Types}

import scala.annotation.unused
import scala.reflect.ClassTag

import dataprism.sql.{AnsiTypes, NullabilityTypeChoice, SelectedType}

trait JdbcAnsiTypes extends AnsiTypes[JdbcCodec]:
  private def tc[A](codec: JdbcCodec[Option[A]])(
      using NullabilityTypeChoice.Nullable[A] =:= Option[A],
      JdbcCodec[Option[A]] =:= JdbcCodec[NullabilityTypeChoice.Nullable[A]]
  ): TypeOf[A] =
    NullabilityTypeChoice.nullableByDefault(codec, _.get)

  private def primitive[A <: AnyRef: ClassTag, B <: AnyVal](
      name: String,
      sqlType: Int,
      unbox: A => B,
      box: B => A
  ): TypeOf[B] =
    tc(JdbcCodec.byClass[A](name, sqlType).imap[Option[B]](_.map(unbox))(_.map(box)))

  val smallint: TypeOf[Short] = primitive[java.lang.Short, Short]("SMALLINT", Types.SMALLINT, _.shortValue(), Short.box)
  val integer: TypeOf[Int]    = primitive[java.lang.Integer, Int]("INTEGER", Types.INTEGER, _.intValue(), Int.box)
  val bigint: TypeOf[Long]    = primitive[java.lang.Long, Long]("BIGINT", Types.BIGINT, _.longValue(), Long.box)
  val real: TypeOf[Float]     = primitive[java.lang.Float, Float]("REAL", Types.REAL, _.floatValue(), Float.box)
  val doublePrecision: TypeOf[Double] =
    primitive[java.lang.Double, Double]("DOUBLE PRECISION", Types.DOUBLE, _.doubleValue(), Double.box)
  val boolean: TypeOf[Boolean] =
    primitive[java.lang.Boolean, Boolean]("BOOLEAN", Types.BOOLEAN, _.booleanValue(), Boolean.box)

  def varchar(n: Int): TypeOf[String] = tc(JdbcCodec.byClass[String](s"VARCHAR($n)", Types.VARCHAR))

  override def defaultStringType: TypeOf[String] = varchar(254)

  val date: TypeOf[Date] = tc(JdbcCodec.byClass[Date]("DATE", Types.DATE))
  val time: TypeOf[Time] = tc(JdbcCodec.byClass[Time]("TIME", Types.TIME))

  val timeWithTimezone: TypeOf[Time] = tc(JdbcCodec.byClass[Time]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE))

  val timestamp: TypeOf[Timestamp] = tc(JdbcCodec.byClass[Timestamp]("TIMESTAMP", Types.TIMESTAMP))

  val timestampWithTimezone: TypeOf[Timestamp] =
    tc(JdbcCodec.byClass[Timestamp]("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE))

  trait ArrayMapping[A]:
    def makeArrayType(inner: SelectedType[A, JdbcCodec]): TypeOf[Seq[A]]

  def ArrayMapping: ArrayMappingCompanion
  trait ArrayMappingCompanion:
    given ArrayMapping[Byte] with
      override def makeArrayType(inner: SelectedType[Byte, JdbcCodec]): TypeOf[Seq[Byte]] =
        tc(
          JdbcCodec.simple(
            "BLOB",
            (a, b) => Option(a.getBytes(b)).map(_.toSeq),
            (a, b, c) => a.setBytes(b, c.fold(null)(_.toArray))
          )
        )

  val blob: TypeOf[Seq[Byte]] = ArrayMapping.given_ArrayMapping_Byte.makeArrayType(
    primitive[java.lang.Byte, Byte]("BYTE", Types.TINYINT, _.byteValue(), Byte.box).notNull
  )

  def array[A](inner: SelectedType[A, JdbcCodec])(using mapping: ArrayMapping[A]): TypeOf[Seq[A]] =
    mapping.makeArrayType(inner)

object JdbcAnsiTypes extends JdbcAnsiTypes:
  override def ArrayMapping: ArrayMappingCompanion = new ArrayMappingCompanion {}
