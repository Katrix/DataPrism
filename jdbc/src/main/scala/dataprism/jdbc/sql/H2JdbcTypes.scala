package dataprism.jdbc.sql

import java.sql.Types
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID
import scala.annotation.tailrec
import scala.util.Using
import cats.syntax.all.*
import dataprism.sql.{NullabilityTypeChoice, Nullable, SelectedType, SqlNull}

trait H2JdbcTypes extends JdbcAnsiTypes:
  private def tc[A](codec: JdbcCodec[A | SqlNull]): TypeOf[A] = NullabilityTypeChoice.nullableByDefault(codec, _.get)

  def arrayOf[A](tpe: SelectedType[JdbcCodec, A]): TypeOfN[A, Seq, tpe.Dimension] =
    val elementCodec = tpe.codec

    given Using.Releasable[java.sql.Array] with {
      override def release(resource: java.sql.Array): Unit = resource.free()
    }

    val elemTypeName = elementCodec.name

    NullabilityTypeChoice.nullableByDefaultDimensional(
      JdbcCodec.withConnection(
        s"$elemTypeName ARRAY",
        rm ?=>
          (rs, i, c) => {
            val ao = rs.getArray(i)
            if ao == null then Right(SqlNull)
            else {
              ao.acquire
              val arrRs = ao.getResultSet
              Seq
                .unfold(arrRs.next()) { cond =>
                  Option.when(cond)(
                    (elementCodec.get(using rm)(arrRs, 2, c), arrRs.next())
                  )
                }
                .sequence
            }
          },
        rm ?=>
          (ps, i, v, c) => {
            @tailrec
            def stripArrayPartOfType(str: String): String =
              if str.endsWith(" ARRAY") then stripArrayPartOfType(str.dropRight(2))
              else str

            ps.setArray(
              i,
              c.createArrayOf(
                stripArrayPartOfType(elemTypeName),
                Nullable.syntax(v).fold(null)(_.asInstanceOf[Seq[A]].map(elementCodec.box).toArray[AnyRef])
              ).acquire
            )
          },
        _.map(elementCodec.box).toArray[AnyRef]
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
