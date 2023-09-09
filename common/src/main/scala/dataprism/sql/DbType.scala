package dataprism.sql

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet}
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

import scala.annotation.unused
import scala.reflect.ClassTag
import scala.util.{NotGiven, Using}

class DbType[A] private (
    val name: String,
    val get: Using.Manager ?=> (ResultSet, Int, Connection) => A,
    val set: Using.Manager ?=> (PreparedStatement, Int, A, Connection) => Unit,
    val isNullable: Boolean = false
)
object DbType {
  def simple[A](
      name: String,
      get: (ResultSet, Int) => A,
      set: (PreparedStatement, Int, A) => Unit,
      isNullable: Boolean = false
  ): DbType[A] =
    DbType(name, (a, b, _) => get(a, b), (a, b, c, _) => set(a, b, c), isNullable)

  def withConnection[A](
      name: String,
      get: Using.Manager ?=> (ResultSet, Int, Connection) => A,
      set: Using.Manager ?=> (PreparedStatement, Int, A, Connection) => Unit,
      isNullable: Boolean = false
  ): DbType[A] = DbType(name, get, set, isNullable)

  val int1: DbType[Byte]       = DbType.simple("INT1", _.getByte(_), _.setByte(_, _))
  val int2: DbType[Short]      = DbType.simple("INT2", _.getShort(_), _.setShort(_, _))
  val int4: DbType[Int]        = DbType.simple("INT4", _.getInt(_), _.setInt(_, _))
  val int8: DbType[Long]       = DbType.simple("INT8", _.getLong(_), _.setLong(_, _))
  val float: DbType[Float]     = DbType.simple("REAL", _.getFloat(_), _.setFloat(_, _))
  val double: DbType[Double]   = DbType.simple("DOUBLE PRECISION", _.getDouble(_), _.setDouble(_, _))
  val boolean: DbType[Boolean] = DbType.simple("BOOLEAN", _.getBoolean(_), _.setBoolean(_, _))

  val text: DbType[String] = DbType.simple("TEXT", _.getString(_), _.setString(_, _))
  val uuid: DbType[UUID]   = DbType.simple("UUID", _.getObject(_, classOf[UUID]), _.setObject(_, _))
  val timestamptz: DbType[Instant] =
    DbType.simple(
      "TIMESTAMPTZ",
      _.getObject(_, classOf[OffsetDateTime]).toInstant,
      (a, b, c) => a.setObject(b, c.atOffset(ZoneOffset.UTC))
    )

  trait ArrayMapping[A]:
    def makeArrayType(inner: DbType[A]): DbType[Seq[A]]

  object ArrayMapping extends LowPriorityArrayMappings:
    given ArrayMapping[Byte] with
      override def makeArrayType(inner: DbType[Byte]): DbType[Seq[Byte]] =
        DbType.simple("BYTEA", _.getBytes(_).toSeq, (a, b, c) => a.setBytes(b, c.toArray))

  trait LowPriorityArrayMappings:
    given[A](using @unused ev: NotGiven[A <:< Seq[_]]): ArrayMapping[A] with
      override def makeArrayType(inner: DbType[A]): DbType[Seq[A]] = DbType.withConnection(
        s"ARRAY ${inner.name}",
        (rs: ResultSet, i: Int, _: Connection) => rs.getArray(i).getArray.asInstanceOf[Array[A]].toSeq,
        (ps: PreparedStatement, i: Int, obj: Seq[A], con: Connection) => {
          val arr = con.createArrayOf(inner.name, obj.map(_.asInstanceOf[AnyRef]).toArray)
          ps.setArray(i, arr)
        }
      )

  def array[A](inner: DbType[A])(using mapping: ArrayMapping[A]): DbType[Seq[A]] =
    mapping.makeArrayType(inner)

  def nullable[A](inner: DbType[A])(using @unused ev: NotGiven[A <:< Option[_]]): DbType[Option[A]] = DbType.withConnection(
    inner.name,
    (a, b, c) => Option(inner.get(a, b, c)),
    (a, b, c, d) => inner.set(a, b, c.map(_.asInstanceOf[AnyRef]).orNull.asInstanceOf[A], d),
    isNullable = true
  )
}
