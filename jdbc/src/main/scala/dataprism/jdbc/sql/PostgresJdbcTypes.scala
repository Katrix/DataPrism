package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID
import scala.annotation.unused
import scala.util.NotGiven

trait PostgresJdbcTypes extends JdbcAnsiTypes:

  val text: JdbcType[String] = JdbcType.simple("TEXT", _.getString(_), _.setString(_, _))

  override def defaultStringType: JdbcType[String] = text

  val uuid: JdbcType[UUID] = JdbcType.simple("UUID", _.getObject(_, classOf[UUID]), _.setObject(_, _))

  def ArrayMapping: ArrayMappingCompanion with LowPriorityArrayMappings

  trait LowPriorityArrayMappings:
    given [A](using @unused ev: NotGiven[A <:< Seq[_]]): ArrayMapping[A] with
      override def makeArrayType(inner: JdbcType[A]): JdbcType[Seq[A]] = JdbcType.withConnection(
        s"ARRAY ${inner.name}",
        (rs: ResultSet, i: Int, _: Connection) => rs.getArray(i).getArray.asInstanceOf[Array[A]].toSeq,
        (ps: PreparedStatement, i: Int, obj: Seq[A], con: Connection) => {
          val arr = con.createArrayOf(inner.name, obj.map(_.asInstanceOf[AnyRef]).toArray)
          ps.setArray(i, arr)
        }
      )
      
  object javaTime:
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

object PostgresJdbcTypes extends PostgresJdbcTypes:
  override def ArrayMapping: ArrayMappingCompanion with LowPriorityArrayMappings =
    new ArrayMappingCompanion with LowPriorityArrayMappings {}
