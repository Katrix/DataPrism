package dataprism.sql

import java.time.*
import scala.annotation.unused
import scala.util.NotGiven

trait AnsiTypes[Type[_]] {
  def smallint: Type[Short]
  def integer: Type[Int]
  def bigint: Type[Long]
  def real: Type[Float]
  def doublePrecision: Type[Double]
  
  def varchar(n: Int): Type[String]
  
  def defaultStringType: Type[String]
  
  def date: Type[LocalDate]
  def time: Type[LocalTime]
  def timeWithTimezone: Type[OffsetTime]
  def timestamp: Type[LocalDateTime]
  def timestampWithTimezone: Type[OffsetDateTime]
  
  def boolean: Type[Boolean]
  
  def blob: Type[Seq[Byte]]

  type Nullable[A] = A match {
    case Option[b] => Option[b]
    case _ => Option[A]
  }
  
  def nullable[A](tpe: Type[A])(using @unused ev: NotGiven[A <:< Option[_]]): Type[Nullable[A]]
}
