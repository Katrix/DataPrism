package dataprism

import cats.Apply
import dataprism.sql.SqlNull

trait WithOptionalInstances {

  given [A](using Frac: Fractional[A]): Fractional[Option[A]] with {
    override def div(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.div(a, b))

    override def plus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.plus(a, b))

    override def minus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.minus(a, b))

    override def times(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Frac.times(a, b))

    override def negate(x: Option[A]): Option[A] = x.map(Frac.negate)

    override def fromInt(x: Int): Option[A] = Some(Frac.fromInt(x))

    override def parseString(str: String): Option[Option[A]] = Some(Frac.parseString(str))

    override def toInt(x: Option[A]): Int = x.fold(0)(Frac.toInt)

    override def toLong(x: Option[A]): Long = x.fold(0L)(Frac.toLong)

    override def toFloat(x: Option[A]): Float = x.fold(0F)(Frac.toFloat)

    override def toDouble(x: Option[A]): Double = x.fold(0D)(Frac.toDouble)

    override def compare(x: Option[A], y: Option[A]): Int = x.zip(y).map((a, b) => Frac.compare(a, b)).getOrElse(0)
  }

  given [A](using Integ: Integral[A]): Integral[Option[A]] with {
    override def quot(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.quot(a, b))

    override def rem(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.rem(a, b))

    override def plus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.plus(a, b))

    override def minus(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.minus(a, b))

    override def times(x: Option[A], y: Option[A]): Option[A] = x.zip(y).map((a, b) => Integ.times(a, b))

    override def negate(x: Option[A]): Option[A] = x.map(Integ.negate)

    override def fromInt(x: Int): Option[A] = Some(Integ.fromInt(x))

    override def parseString(str: String): Option[Option[A]] = Some(Integ.parseString(str))

    override def toInt(x: Option[A]): Int = x.fold(0)(Integ.toInt)

    override def toLong(x: Option[A]): Long = x.fold(0L)(Integ.toLong)

    override def toFloat(x: Option[A]): Float = x.fold(0F)(Integ.toFloat)

    override def toDouble(x: Option[A]): Double = x.fold(0D)(Integ.toDouble)

    override def compare(x: Option[A], y: Option[A]): Int = x.zip(y).map((a, b) => Integ.compare(a, b)).getOrElse(0)
  }

  import cats.syntax.all.*
  import dataprism.sql.Nullable

  given [A](using order: cats.Order[A]): cats.Order[A | SqlNull] with {
    override def compare(x: A | SqlNull, y: A | SqlNull): Int = (x, y) match {
      case (SqlNull, SqlNull) => 0
      case (SqlNull, _)       => -1
      case (_, SqlNull)       => 1
      case (a, b)             => order.compare(Nullable.syntax(a).unsafeGet, Nullable.syntax(b).unsafeGet)
    }
  }

  given [A](using ordering: Ordering[A]): Ordering[A | SqlNull] with {
    override def compare(x: Nullable[A], y: Nullable[A]): Int =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => ordering.compare(a, b)).toOption.getOrElse(0)
  }

  given [A](using Frac: Fractional[A]): Fractional[Nullable[A]] with {
    override def div(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Frac.div(a, b)).orSqlNull

    override def plus(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Frac.plus(a, b)).orSqlNull

    override def minus(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Frac.minus(a, b)).orSqlNull

    override def times(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Frac.times(a, b)).orSqlNull

    override def negate(x: Nullable[A]): Nullable[A] = Nullable.syntax(x).map(Frac.negate).orSqlNull

    override def fromInt(x: Int): Nullable[A] = Frac.fromInt(x)

    override def parseString(str: String): Option[Nullable[A]] = Frac.parseString(str)

    override def toInt(x: Nullable[A]): Int = Nullable.syntax(x).fold(0)(Frac.toInt)

    override def toLong(x: Nullable[A]): Long = Nullable.syntax(x).fold(0L)(Frac.toLong)

    override def toFloat(x: Nullable[A]): Float = Nullable.syntax(x).fold(0F)(Frac.toFloat)

    override def toDouble(x: Nullable[A]): Double = Nullable.syntax(x).fold(0D)(Frac.toDouble)

    override def compare(x: Nullable[A], y: Nullable[A]): Int =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Frac.compare(a, b)).toOption.getOrElse(0)
  }

  given [A](using Integ: Integral[A]): Integral[Nullable[A]] with {
    override def quot(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Integ.quot(a, b)).orSqlNull

    override def rem(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Integ.rem(a, b)).orSqlNull

    override def plus(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Integ.plus(a, b)).orSqlNull

    override def minus(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Integ.minus(a, b)).orSqlNull

    override def times(x: Nullable[A], y: Nullable[A]): Nullable[A] =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Integ.times(a, b)).orSqlNull

    override def negate(x: Nullable[A]): Nullable[A] = Nullable.syntax(x).map(Integ.negate).orSqlNull

    override def fromInt(x: Int): Nullable[A] = Integ.fromInt(x)

    override def parseString(str: String): Option[Nullable[A]] = Integ.parseString(str)

    override def toInt(x: Nullable[A]): Int = Nullable.syntax(x).fold(0)(Integ.toInt)

    override def toLong(x: Nullable[A]): Long = Nullable.syntax(x).fold(0L)(Integ.toLong)

    override def toFloat(x: Nullable[A]): Float = Nullable.syntax(x).fold(0F)(Integ.toFloat)

    override def toDouble(x: Nullable[A]): Double = Nullable.syntax(x).fold(0D)(Integ.toDouble)

    override def compare(x: Nullable[A], y: Nullable[A]): Int =
      Nullable.syntax(x).map2(Nullable.syntax(y))((a, b) => Integ.compare(a, b)).toOption.getOrElse(0)
  }
  
  given Apply[Nullable] with {
    override def ap[A, B](ff: Nullable[A => B])(fa: Nullable[A]): Nullable[B] =
      Nullable.syntax(ff).flatMap(f => Nullable.syntax(fa).map(f)).orSqlNull

    override def map[A, B](fa: Nullable[A])(f: A => B): Nullable[B] = Nullable.syntax(fa).map(f).orSqlNull
  }
}
