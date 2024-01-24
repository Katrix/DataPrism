package dataprism.skunk.sql

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

import dataprism.sql.NullabilityTypeChoice
import scodec.bits.BitVector
import skunk.Codec
import skunk.codec.all
import skunk.data.{Arr, LTree, Type}

object SkunkTypes {
  type TypeOf[A] = NullabilityTypeChoice[Codec, A]

  extension [A](skunkCodec: Codec[A])
    def wrap: TypeOf[A] =
      if skunkCodec.types.length != 1 then
        throw new IllegalArgumentException("Skunk types must always have only one SQL type")
      NullabilityTypeChoice.notNullByDefault(skunkCodec, _.opt)

  // =============== Numerics ===============

  val int2: TypeOf[Short] = all.int2.wrap
  val int4: TypeOf[Int]   = all.int4.wrap
  val int8: TypeOf[Long]  = all.int8.wrap

  val numeric: TypeOf[BigDecimal]                                 = all.numeric.wrap
  def numeric(precision: Int, scale: Int = 0): TypeOf[BigDecimal] = all.numeric(precision, scale).wrap

  val float4: TypeOf[Float]  = all.float4.wrap
  val float8: TypeOf[Double] = all.float8.wrap

  val _int2: TypeOf[Arr[Short]]         = all._int2.wrap
  val _int4: TypeOf[Arr[Int]]           = all._int4.wrap
  val _int8: TypeOf[Arr[Long]]          = all._int8.wrap
  val _numeric: TypeOf[Arr[BigDecimal]] = all._numeric.wrap
  val _float4: TypeOf[Arr[Float]]       = all._float4.wrap
  val _float8: TypeOf[Arr[Double]]      = all._float8.wrap

  // =============== Text ===============

  val varchar: TypeOf[String]         = all.varchar.wrap
  def varchar(n: Int): TypeOf[String] = all.varchar(n).wrap

  val bpchar: TypeOf[String]         = all.bpchar.wrap
  def bpchar(n: Int): TypeOf[String] = all.bpchar(n).wrap

  val name: TypeOf[String] = all.name.wrap
  val text: TypeOf[String] = all.text.wrap

  val _varchar: TypeOf[Arr[String]] = all._varchar.wrap
  val _bpchar: TypeOf[Arr[String]]  = all._bpchar.wrap
  val _name: TypeOf[Arr[String]]    = all._name.wrap
  val _text: TypeOf[Arr[String]]    = all._text.wrap

  // =============== Temporal ===============

  val date: TypeOf[LocalDate] = all.date.wrap

  val time: TypeOf[LocalTime]                 = all.time.wrap
  def time(precision: Int): TypeOf[LocalTime] = all.time(precision).wrap

  val timetz: TypeOf[OffsetTime]                 = all.timetz.wrap
  def timetz(precision: Int): TypeOf[OffsetTime] = all.timetz(precision).wrap

  val timestamp: TypeOf[LocalDateTime]                 = all.timestamp.wrap
  def timestamp(precision: Int): TypeOf[LocalDateTime] = all.timestamp(precision).wrap

  val timestamptz: TypeOf[OffsetDateTime]                 = all.timestamptz.wrap
  def timestamptz(precision: Int): TypeOf[OffsetDateTime] = all.timestamptz(precision).wrap

  val interval: TypeOf[Duration]                 = all.interval.wrap
  def interval(precision: Int): TypeOf[Duration] = all.interval(precision).wrap

  // =============== Boolean ===============

  val bool: TypeOf[Boolean] = all.bool.wrap

  // =============== Enum ===============

  def `enum`[A](encode: A => String, decode: String => Option[A], tpe: Type): TypeOf[A] =
    all.`enum`(encode, decode, tpe).wrap

  // =============== UUID ===============

  val uuid: TypeOf[UUID] = all.uuid.wrap

  // =============== Binary ===============

  val bytea: TypeOf[Array[Byte]] = all.bytea.wrap

  val bit: TypeOf[BitVector]              = all.bit.wrap
  def bit(length: Int): TypeOf[BitVector] = all.bit(length).wrap

  val varbit: TypeOf[BitVector]              = all.varbit.wrap
  def varbit(length: Int): TypeOf[BitVector] = all.varbit(length).wrap

  // =============== LTree ===============

  val ltree: TypeOf[LTree] = all.ltree.wrap
}
