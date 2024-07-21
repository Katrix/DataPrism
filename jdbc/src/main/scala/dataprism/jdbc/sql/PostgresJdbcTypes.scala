package dataprism.jdbc.sql

import java.sql.Types
import java.time.*
import java.util.UUID

import scala.annotation.tailrec
import scala.util.Using

import cats.syntax.all.*
import dataprism.sql.{NullabilityTypeChoice, SelectedType}

trait PostgresJdbcTypes extends JdbcAnsiTypes:
  private def tc[A](codec: JdbcCodec[Option[A]]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  val text: TypeOf[String] = tc(
    JdbcCodec.withWasNullCheck[String]("TEXT", Types.VARCHAR, _.getString(_), _.setString(_, _))
  )

  def arrayOf[A](tpe: SelectedType[JdbcCodec, A]): TypeOfN[A, Seq, tpe.Dimension] =
    val elementCodec = tpe.codec

    given Using.Releasable[java.sql.Array] with {
      override def release(resource: java.sql.Array): Unit = resource.free()
    }

    val elemTypeName = elementCodec.name

    NullabilityTypeChoice.nullableByDefaultDimensional(
      JdbcCodec.withConnection(
        s"$elemTypeName[]",
        rm ?=>
          (rs, i, c) =>
            Option(rs.getArray(i))
              .map { arr =>
                arr.acquire
                val arrRs = arr.getResultSet
                Seq.unfold(arrRs.next()) { cond =>
                  Option.when(cond)(
                    (elementCodec.get(using rm)(arrRs, 2, c), arrRs.next())
                  )
                }
              }
              .traverse(_.sequence),
        rm ?=>
          (ps, i, v, c) => {
            @tailrec
            def stripArrayPartOfType(str: String): String =
              if str.endsWith("[]") then stripArrayPartOfType(str.dropRight(2))
              else str

            // FIXME: Nested arrays broken
            ps.setArray(i, c.createArrayOf(stripArrayPartOfType(elemTypeName), v.fold(null)(_.toArray[Any])).acquire)
          }
      ),
      _.get,
      tpe
    )

  override def defaultStringType: TypeOf[String] = text

  val uuid: TypeOf[UUID] = tc(JdbcCodec.byClass[UUID]("UUID", 2950))

  object javaTime:
    val date: TypeOf[LocalDate] = tc(JdbcCodec.byClass[LocalDate]("DATE", Types.DATE))
    val time: TypeOf[LocalTime] = tc(JdbcCodec.byClass[LocalTime]("TIME", Types.TIME))
    val timeWithTimezone: TypeOf[OffsetTime] =
      tc(JdbcCodec.byClass[OffsetTime]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE))

    val timestamp: TypeOf[LocalDateTime] = tc(JdbcCodec.byClass[LocalDateTime]("TIMESTAMP", Types.TIMESTAMP))

    val timestampWithTimezone: TypeOf[OffsetDateTime] =
      tc(JdbcCodec.byClass[OffsetDateTime]("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE))

object PostgresJdbcTypes extends PostgresJdbcTypes
