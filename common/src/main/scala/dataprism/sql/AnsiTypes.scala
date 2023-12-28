package dataprism.sql

import java.sql.{Date, Time, Timestamp}
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
  
  def date: Type[Date]
  def time: Type[Time]
  def timeWithTimezone: Type[Time]
  def timestamp: Type[Timestamp]
  def timestampWithTimezone: Type[Timestamp]
  
  def boolean: Type[Boolean]
  
  def blob: Type[Seq[Byte]]

  type Nullable[A] = A match {
    case Option[b] => Option[b]
    case _ => Option[A]
  }
  
  def nullable[A](tpe: Type[A])(using @unused ev: NotGiven[A <:< Option[_]]): Type[Nullable[A]]
}
