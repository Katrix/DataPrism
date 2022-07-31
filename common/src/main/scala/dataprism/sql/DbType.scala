package dataprism.sql

import java.sql.{JDBCType, PreparedStatement, ResultSet}
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

import scala.reflect.ClassTag

case class DbType[A](
    name: String,
    get: (ResultSet, Int) => A,
    set: (PreparedStatement, Int, A) => Unit
)
object DbType {
  val int8: DbType[Byte]       = DbType("INT8", _.getByte(_), _.setByte(_, _))
  val int16: DbType[Short]     = DbType("INT16", _.getShort(_), _.setShort(_, _))
  val int32: DbType[Int]       = DbType("INT32", _.getInt(_), _.setInt(_, _))
  val int64: DbType[Long]      = DbType("INT64", _.getLong(_), _.setLong(_, _))
  val float: DbType[Float]     = DbType("REAL", _.getFloat(_), _.setFloat(_, _))
  val double: DbType[Double]   = DbType("DOUBLE PRECISION", _.getDouble(_), _.setDouble(_, _))
  val boolean: DbType[Boolean] = DbType("BOOLEAN", _.getBoolean(_), _.setBoolean(_, _))

  val text: DbType[String] = DbType("TEXT", _.getString(_), _.setString(_, _))
  val uuid: DbType[UUID]   = DbType("UUID", _.getObject(_, classOf[UUID]), _.setObject(_, _))
  val timestamptz: DbType[Instant] =
    DbType(
      "TIMESTAMPTZ",
      _.getObject(_, classOf[OffsetDateTime]).toInstant,
      (a, b, c) => a.setObject(b, c.atOffset(ZoneOffset.UTC))
    )

  def array[A: ClassTag](inner: DbType[A]): DbType[Seq[A]] =
    DbType(s"ARRAY ${inner.name}", _.getObject(_).asInstanceOf[Array[A]].toSeq, (a, b, c) => a.setObject(b, c.toArray))
}
