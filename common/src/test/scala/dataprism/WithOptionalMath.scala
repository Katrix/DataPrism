package dataprism

trait WithOptionalMath {

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
}
