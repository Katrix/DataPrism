package dataprism.sql

import java.sql.{Date, Time, Timestamp}

trait AnsiTypes[Codec[_]] {
  type TypeOf[A] = NullabilityTypeChoice[Codec, A]

  def smallint: TypeOf[Short]
  def integer: TypeOf[Int]
  def bigint: TypeOf[Long]
  def real: TypeOf[Float]
  def doublePrecision: TypeOf[Double]

  def decimal: TypeOf[BigDecimal]

  def decimalN(m: Int): TypeOf[BigDecimal]

  def decimalN(m: Int, n: Int): TypeOf[BigDecimal]

  def varchar(n: Int): TypeOf[String]

  def defaultStringType: TypeOf[String]

  def date: TypeOf[Date]
  def time: TypeOf[Time]
  def timeWithTimezone: TypeOf[Time]
  def timestamp: TypeOf[Timestamp]
  def timestampWithTimezone: TypeOf[Timestamp]

  def boolean: TypeOf[Boolean]

  def blob: TypeOf[Seq[Byte]]
}
