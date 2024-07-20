package dataprism.jdbc.sql

import java.sql.Types
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

import scala.util.Using

import cats.syntax.all.*
import dataprism.sql.{NullabilityTypeChoice, SelectedType}

trait H2JdbcTypes extends JdbcAnsiTypes:
  private def tc[A](codec: JdbcCodec[Option[A]]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  def arrayOf[A](tpe: SelectedType[JdbcCodec, A]): TypeOfN[A, Seq, tpe.Dimension] =
    val elementCodec = tpe.codec

    given Using.Releasable[java.sql.Array] with {
      override def release(resource: java.sql.Array): Unit = resource.free()
    }

    NullabilityTypeChoice.nullableByDefaultDimensional(
      JdbcCodec.withConnection(
        s"${elementCodec.name}[]",
        rm ?=>
          (rs, i, c) =>
            Option(rs.getArray(i))
              .map { arr =>
                arr.acquire
                val arrRs = arr.getResultSet
                Seq.unfold((arrRs.next(), 1)) { (cond, i) =>
                  Option.when(cond)(
                    (elementCodec.get(using rm)(arrRs, i, c), (arrRs.next(), i + 1))
                  )
                }
              }
              .traverse(_.sequence),
        rm ?=>
          (ps, i, v, c) => {
            ps.setArray(i, c.createArrayOf(elementCodec.name, v.fold(null)(_.toArray[Any])).acquire)
          }
      ),
      _.get,
      tpe
    )

  val uuid: TypeOf[UUID]    = tc(JdbcCodec.byClass[UUID]("UUID", 2950))
  val tinyint: TypeOf[Byte] = tc(JdbcCodec.withWasNullCheck("TINYINT", Types.TINYINT, _.getByte(_), _.setByte(_, _)))

  object javaTime:
    val date: TypeOf[LocalDate] = tc(JdbcCodec.byClass[LocalDate]("DATE", Types.DATE))
    val time: TypeOf[LocalTime] = tc(JdbcCodec.byClass[LocalTime]("TIME", Types.TIME))
    val timeWithTimezone: TypeOf[OffsetTime] =
      tc(JdbcCodec.byClass[OffsetTime]("TIME WITH TIMEZONE", Types.TIME_WITH_TIMEZONE))

    val timestamp: TypeOf[LocalDateTime] = tc(JdbcCodec.byClass[LocalDateTime]("TIMESTAMP", Types.TIMESTAMP))

    val timestampWithTimezone: TypeOf[OffsetDateTime] =
      tc(JdbcCodec.byClass[OffsetDateTime]("TIMESTAMP WITH TIMEZONE", Types.TIMESTAMP_WITH_TIMEZONE))

object H2JdbcTypes extends H2JdbcTypes
