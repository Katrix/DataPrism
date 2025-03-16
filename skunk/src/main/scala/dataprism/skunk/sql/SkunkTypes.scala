package dataprism.skunk.sql

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

import cats.syntax.all.*
import dataprism.sql.{
  NullabilityTypeChoice,
  NullabilityTypeChoiceArr,
  NullabilityTypeChoiceNoArr,
  SelectedType,
  SqlNull
}
import scodec.bits.BitVector
import skunk.Codec
import skunk.codec.all
import skunk.data.{Arr, LTree, Type}

object SkunkTypes {
  type TypeOf[A]            = NullabilityTypeChoiceNoArr[Codec, A]
  type TypeOfN[A, N <: Int] = NullabilityTypeChoiceArr[Codec, Seq, A, N]

  extension [A](skunkCodec: Codec[A])
    def wrap: TypeOf[A] =
      if skunkCodec.types.length != 1 then
        throw new IllegalArgumentException("Skunk types must always have only one SQL type")
      import dataprism.sql.sqlNullSyntax.*
      NullabilityTypeChoice.notNullByDefault(
        skunkCodec,
        _.opt.imap[A | SqlNull](_.getOrElse(SqlNull))(_.toOption)
      )

  private val escapeRegex = """[{}"\\\s]|NULL""".r.unanchored

  private def decodeArray[A](
      codec: Codec[A],
      str: String,
      from: Int,
      decodeData: Boolean,
      delim: Char = ','
  ): Either[String, (Seq[A], Int)] =
    if str.length < from then Left("Unexpected end of array")
    else if str(from) == '{' && str(from + 1) == '}' then Right((Nil, from + 1))
    else if str(from) != '{' then Left(s"Expected array to start with {, but got ${str(from)}")
    else
      var i        = from + 1
      var c        = str(i)
      val data     = Seq.newBuilder[A]
      var moreData = true

      val sb            = new StringBuilder
      var escapeNext    = false
      var escapedString = false
      var newData       = true

      while moreData do
        if newData && c == '"' then
          newData = false
          escapedString = true
        else if !escapeNext && escapedString && c == '\\' then escapeNext = true
        else if newData && c == '{' then
          newData = false
          val to = decodeArray(codec, str, i, decodeData = false, delim) match
            case Right((_, to)) => to
            case l @ Left(_)    => return l

          sb.append(str.substring(i, to + 1))
          i = to
        else if escapeNext || (if escapedString then c != '"' else c != delim && c != '}') then
          escapeNext = false
          newData = false
          sb.append(c)
        else if if escapedString then c == '"' else c == delim || c == '}' then
          val s = sb.result()

          if decodeData then
            codec.decode(0, List(Option.when(s != "null")(s))) match
              case Right(v)    => data += v
              case Left(value) => return Left(value.message)

          sb.clear()

          if escapedString && c == '"' then
            if str.length < i then return Left("Unexpected end of array")
            i += 1
            c = str(i)

          if c == '}' then moreData = false
          else if c == delim then
            newData = true
            escapedString = false
          else return Left(s"Unexpected character after closing quote $c")
        else return Left(s"Unhandled character $c")
        end if

        if moreData then
          i += 1
          if str.length <= i then return Left("Unexpected end of array")
          c = str(i)
      end while

      Right((data.result(), i))

  private def encodeSingleArray[A](codec: Codec[A], delim: Char = ',')(elems: Seq[A]): String =
    if elems.isEmpty then "{}"
    else
      val sb = new StringBuilder
      sb.append('{')
      var i = 0
      while i < elems.length do
        codec.encode(elems(i)).head match
          case Some(str) =>
            val subelemetIsArray = codec.types.head.componentTypes.nonEmpty
            if (str.isEmpty
                || escapeRegex.matches(str)
                || str.contains(delim)) && !subelemetIsArray
            then
              sb.append("\"")
              sb.append(str.replace("\\", "\\\\").replace("\"", "\\\""))
              sb.append("\"")
            else sb.append(str)
          case None => sb.append("NULL")

        i += 1
        if i != elems.length then sb.append(delim)

      sb.append('}')
      val r = sb.toString
      r

  def arrayOf[A](tpe: SelectedType[Codec, A]): TypeOfN[A, tpe.Dimension] =
    val headType = tpe.codec.types.head
    import dataprism.sql.sqlNullSyntax.*
    NullabilityTypeChoice.notNullByDefaultDimensional(
      Codec
        .simple(
          encodeSingleArray(tpe.codec),
          str => decodeArray(tpe.codec, str, from = 0, decodeData = true).map(_._1),
          if headType.componentTypes.nonEmpty then headType else Type("_" + headType.name, List(headType))
        ),
      _.opt.imap[Seq[A] | SqlNull](_.getOrElse(SqlNull))(_.toOption),
      tpe
    )

  // =============== Numerics ===============

  val int2: TypeOf[Short] = all.int2.wrap
  val int4: TypeOf[Int]   = all.int4.wrap
  val int8: TypeOf[Long]  = all.int8.wrap

  val numeric: TypeOf[BigDecimal]                                 = all.numeric.wrap
  def numeric(precision: Int, scale: Int = 0): TypeOf[BigDecimal] = all.numeric(precision, scale).wrap

  val float4: TypeOf[Float]  = all.float4.wrap
  val float8: TypeOf[Double] = all.float8.wrap

  val _int2: TypeOfN[Short, 0]         = arrayOf(int2)
  val _int4: TypeOfN[Int, 0]           = arrayOf(int4)
  val _int8: TypeOfN[Long, 0]          = arrayOf(int8)
  val _numeric: TypeOfN[BigDecimal, 0] = arrayOf(numeric)
  val _float4: TypeOfN[Float, 0]       = arrayOf(float4)
  val _float8: TypeOfN[Double, 0]      = arrayOf(float8)

  all._int4

  // =============== Text ===============

  val varchar: TypeOf[String]         = all.varchar.wrap
  def varchar(n: Int): TypeOf[String] = all.varchar(n).wrap

  val bpchar: TypeOf[String]         = all.bpchar.wrap
  def bpchar(n: Int): TypeOf[String] = all.bpchar(n).wrap

  val name: TypeOf[String] = all.name.wrap
  val text: TypeOf[String] = all.text.wrap

  val _varchar: TypeOfN[String, 0] = arrayOf(varchar)
  val _bpchar: TypeOfN[String, 0]  = arrayOf(bpchar)
  val _name: TypeOfN[String, 0]    = arrayOf(name)
  val _text: TypeOfN[String, 0]    = arrayOf(text)

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
