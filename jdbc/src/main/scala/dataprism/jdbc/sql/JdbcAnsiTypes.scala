package dataprism.jdbc.sql

import java.sql.{Connection, Date, PreparedStatement, ResultSet, Time, Timestamp, Types}

import scala.annotation.unused
import scala.reflect.ClassTag

import dataprism.sql.{AnsiTypes, NullabilityTypeChoice, SelectedType}

trait JdbcAnsiTypes extends AnsiTypes[JdbcCodec]:
  private def tc[A](codec: JdbcCodec[Option[A]]): TypeOf[A] =
    NullabilityTypeChoice.nullableByDefault(codec, _.get)

  private def withWasNullCheck[B](
      name: String,
      sqlType: Int,
      get: (ResultSet, Int) => B,
      set: (PreparedStatement, Int, B) => Unit
  ): TypeOf[B] =
    tc(JdbcCodec.withWasNullCheck(name, sqlType, get, set))

  val smallint: TypeOf[Short] = withWasNullCheck("SMALLINT", Types.SMALLINT, _.getShort(_), _.setShort(_, _))
  val integer: TypeOf[Int]    = withWasNullCheck("INTEGER", Types.INTEGER, _.getInt(_), _.setInt(_, _))
  val bigint: TypeOf[Long]    = withWasNullCheck("BIGINT", Types.BIGINT, _.getLong(_), _.setLong(_, _))
  val real: TypeOf[Float]     = withWasNullCheck("REAL", Types.REAL, _.getFloat(_), _.setFloat(_, _))
  val doublePrecision: TypeOf[Double] =
    withWasNullCheck[Double]("DOUBLE PRECISION", Types.DOUBLE, _.getDouble(_), _.setDouble(_, _))
  val boolean: TypeOf[Boolean] = withWasNullCheck("BOOLEAN", Types.BOOLEAN, _.getBoolean(_), _.setBoolean(_, _))

  def decimal: TypeOf[BigDecimal] =
    withWasNullCheck(s"NUMERIC", Types.DECIMAL, _.getBigDecimal(_), _.setBigDecimal(_, _))
      .imap(jbd => scala.math.BigDecimal(jbd))(bd => bd.bigDecimal)

  def decimalN(m: Int): TypeOf[BigDecimal] =
    withWasNullCheck(s"DECIMAL($m)", Types.DECIMAL, _.getBigDecimal(_), _.setBigDecimal(_, _))
      .imap(jbd => scala.math.BigDecimal(jbd))(bd => bd.bigDecimal)

  def decimalN(m: Int, n: Int): TypeOf[BigDecimal] =
    withWasNullCheck(s"DECIMAL($m, $n)", Types.DECIMAL, _.getBigDecimal(_), _.setBigDecimal(_, _))
      .imap(jbd => scala.math.BigDecimal(jbd))(bd => bd.bigDecimal)

  def varchar(n: Int): TypeOf[String] =
    withWasNullCheck(s"VARCHAR($n)", Types.VARCHAR, _.getString(_), _.setString(_, _))

  override def defaultStringType: TypeOf[String] = varchar(254)

  val date: TypeOf[Date] = withWasNullCheck("DATE", Types.DATE, _.getDate(_), _.setDate(_, _))
  val time: TypeOf[Time] = withWasNullCheck("TIME", Types.TIME, _.getTime(_), _.setTime(_, _))

  val timeWithTimezone: TypeOf[Time] =
    withWasNullCheck[Time]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE, _.getTime(_), _.setTime(_, _))

  val timestamp: TypeOf[Timestamp] =
    withWasNullCheck[Timestamp]("TIMESTAMP", Types.TIMESTAMP, _.getTimestamp(_), _.setTimestamp(_, _))

  val timestampWithTimezone: TypeOf[Timestamp] =
    withWasNullCheck("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE, _.getTimestamp(_), _.setTimestamp(_, _))

  lazy val blob: TypeOf[Seq[Byte]] =
    withWasNullCheck("BYTEA", Types.BLOB, _.getBytes(_), _.setBytes(_, _)).imap(_.toSeq)(_.toArray)

object JdbcAnsiTypes extends JdbcAnsiTypes
