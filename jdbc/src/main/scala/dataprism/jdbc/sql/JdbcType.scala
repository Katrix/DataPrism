package dataprism.jdbc.sql

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet}
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

import scala.annotation.unused
import scala.reflect.ClassTag
import scala.util.{NotGiven, Using}

class JdbcType[A] private (
    val name: String,
    val get: Using.Manager ?=> (ResultSet, Int, Connection) => A,
    val set: Using.Manager ?=> (PreparedStatement, Int, A, Connection) => Unit,
    val isNullable: Boolean = false
):

  def imap[B](f: A => B)(g: B => A): JdbcType[B] =
    JdbcType(name, (a, b, c) => f(this.get(a, b, c)), (a, b, c, d) => this.set(a, b, g(c), d))

object JdbcType {
  def simple[A](
      name: String,
      get: (ResultSet, Int) => A,
      set: (PreparedStatement, Int, A) => Unit,
      isNullable: Boolean = false
  ): JdbcType[A] =
    JdbcType(name, (a, b, _) => get(a, b), (a, b, c, _) => set(a, b, c), isNullable)

  def withConnection[A](
      name: String,
      get: Using.Manager ?=> (ResultSet, Int, Connection) => A,
      set: Using.Manager ?=> (PreparedStatement, Int, A, Connection) => Unit,
      isNullable: Boolean = false
  ): JdbcType[A] = JdbcType(name, get, set, isNullable)
}
