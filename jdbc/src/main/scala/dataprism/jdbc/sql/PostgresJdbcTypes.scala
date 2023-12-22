package dataprism.jdbc.sql

import java.sql.{Connection, PreparedStatement, ResultSet}
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

object PostgresJdbcTypes extends PostgresJdbcTypes:
  override def ArrayMapping: ArrayMappingCompanion with LowPriorityArrayMappings =
    new ArrayMappingCompanion with LowPriorityArrayMappings {}
