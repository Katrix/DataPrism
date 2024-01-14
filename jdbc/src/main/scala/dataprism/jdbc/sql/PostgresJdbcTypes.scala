package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, ResultSet, Types}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

import scala.annotation.unused
import scala.util.NotGiven

import dataprism.sql.{NullabilityTypeChoice, SelectedType}

trait PostgresJdbcTypes extends JdbcAnsiTypes:
  private def tc[A](codec: JdbcCodec[Option[A]]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  val text: TypeOf[String] = tc(JdbcCodec.byClass[String]("TEXT", 25))

  override def defaultStringType: TypeOf[String] = text

  val uuid: TypeOf[UUID] = tc(JdbcCodec.byClass[UUID]("UUID", 2950))

  val ArrayMapping: ArrayMappingCompanion with LowPriorityArrayMappings
  
  trait LowPriorityArrayMappings:
    // TODO: Null handling
    given [A](using @unused ev: NotGiven[A <:< Seq[_]]): ArrayMapping[A] with
      override def makeArrayType(inner: SelectedType[JdbcCodec, A]): TypeOf[Seq[A]] = tc(
        JdbcCodec.withConnection(
          s"ARRAY ${inner.codec.name}",
          (rs: ResultSet, i: Int, _: Connection) =>
            Right(Option(rs.getArray(i)).map(_.getArray.asInstanceOf[Array[A]].toSeq)),
          (ps: PreparedStatement, i: Int, obj: Option[Seq[A]], con: Connection) => {
            val arr = con.createArrayOf(inner.codec.name, obj.fold(null)(_.map(_.asInstanceOf[AnyRef]).toArray))
            ps.setArray(i, arr)
          }
        )
      )

  object javaTime:
    val date: TypeOf[LocalDate] = tc(JdbcCodec.byClass[LocalDate]("DATE", Types.DATE))
    val time: TypeOf[LocalTime] = tc(JdbcCodec.byClass[LocalTime]("TIME", Types.TIME))
    val timeWithTimezone: TypeOf[OffsetTime] =
      tc(JdbcCodec.byClass[OffsetTime]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE))

    val timestamp: TypeOf[LocalDateTime] = tc(JdbcCodec.byClass[LocalDateTime]("TIMESTAMP", Types.TIMESTAMP))

    val timestampWithTimezone: TypeOf[OffsetDateTime] =
      tc(JdbcCodec.byClass[OffsetDateTime]("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE))

object PostgresJdbcTypes extends PostgresJdbcTypes:
  override val ArrayMapping: ArrayMappingCompanion with LowPriorityArrayMappings =
    new ArrayMappingCompanion with LowPriorityArrayMappings {}
