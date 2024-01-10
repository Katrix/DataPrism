package dataprism.jdbc.sql
import java.sql.{Date, Time, Types}

import dataprism.sql.NullabilityTypeChoice

case class MySqlJdbcTypeCastable[A](name: String, tpe: NullabilityTypeChoice[A, JdbcCodec])
trait MySqlJdbcTypes extends JdbcAnsiTypes:
  self =>
  private def tc[A](codec: JdbcCodec[Option[A]])(
      using NullabilityTypeChoice.Nullable[A] =:= Option[A],
      JdbcCodec[Option[A]] =:= JdbcCodec[NullabilityTypeChoice.Nullable[A]]
  ): TypeOf[A] =
    NullabilityTypeChoice.nullableByDefault(codec, _.get)

  val text: TypeOf[String] = tc(JdbcCodec.byClass[String]("TEXT", Types.VARCHAR))

  val decimal: TypeOf[BigDecimal] = tc(
    JdbcCodec.byClass[java.math.BigDecimal]("DECIMAL", Types.DECIMAL)
  ).imap(jbd => scala.math.BigDecimal(jbd))(bd => bd.bigDecimal)

  def decimalN(m: Int): TypeOf[BigDecimal] = tc(
    JdbcCodec.byClass[java.math.BigDecimal](s"DECIMAL($m)", Types.DECIMAL)
  ).imap(jbd => scala.math.BigDecimal(jbd))(bd => bd.bigDecimal)

  def decimalN(m: Int, n: Int): TypeOf[BigDecimal] = tc(
    JdbcCodec.byClass[java.math.BigDecimal](s"DECIMAL($m, $n)", Types.DECIMAL)
  ).imap(jbd => scala.math.BigDecimal(jbd))(bd => bd.bigDecimal)

  object castType:
    val binary: MySqlJdbcTypeCastable[Seq[Byte]] = MySqlJdbcTypeCastable("BINARY", self.blob)
    // def binaryN(n: Int): MySqlJdbcTypeCastable[Seq[Byte]] = MySqlJdbcTypeCastable(s"BINARY($n)", self.varbinary(n))

    val char: MySqlJdbcTypeCastable[String] = MySqlJdbcTypeCastable("CHAR", text) // Not quite accurate, but good enough
    // def charN(n: Int): MySqlJdbcTypeCastable[Seq[Byte]] = MySqlJdbcTypeCastable(s"CHAR($n)", varchar(n))

    val date: MySqlJdbcTypeCastable[Date] = MySqlJdbcTypeCastable("DATE", self.date)

    // val datetime: MySqlJdbcTypeCastable[Option[Timestamp]] = MySqlJdbcTypeCastable("DATETIME", self.datetime)
    // def datetimeN(m: Int): MySqlJdbcTypeCastable[Option[Timestamp]] = MySqlJdbcTypeCastable(s"DATETIME($m)", self.datetime(m))

    val decimal: MySqlJdbcTypeCastable[BigDecimal]          = MySqlJdbcTypeCastable("DECIMAL", self.decimal)
    def decimalN(m: Int): MySqlJdbcTypeCastable[BigDecimal] = MySqlJdbcTypeCastable(s"DECIMAL($m)", self.decimalN(m))
    def decimalN(m: Int, d: Int): MySqlJdbcTypeCastable[BigDecimal] =
      MySqlJdbcTypeCastable(s"DECIMAL($m, $d)", self.decimalN(m, d))

    // val json: MySqlJdbcTypeCastable[Json] = MySqlJdbcTypeCastable("JSON", self.json)

    val nchar: MySqlJdbcTypeCastable[String]          = MySqlJdbcTypeCastable("NCHAR", self.text)
    def ncharN(n: Int): MySqlJdbcTypeCastable[String] = MySqlJdbcTypeCastable("NCHAR", self.varchar(n))

    val signedInteger: MySqlJdbcTypeCastable[Long] = MySqlJdbcTypeCastable("SIGNED INTEGER", self.bigint)

    val time: MySqlJdbcTypeCastable[Time] = MySqlJdbcTypeCastable("TIME", self.time)
    // def timeN(m: Int): MySqlJdbcTypeCastable[Time] = MySqlJdbcTypeCastable(s"TIME($m)", self.timeN(m))

    val unsignedInteger: MySqlJdbcTypeCastable[Long] = MySqlJdbcTypeCastable("UNSIGNED INTEGER", self.bigint)

object MySqlJdbcTypes extends MySqlJdbcTypes:
  override def ArrayMapping: ArrayMappingCompanion =
    new ArrayMappingCompanion {}
