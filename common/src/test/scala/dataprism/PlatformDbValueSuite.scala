package dataprism

import scala.util.NotGiven

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.NullabilityTypeChoice
import munit.{FunSuite, Location}
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen}

trait PlatformDbValueSuite[F[_]: MonadThrow, Codec0[_]] extends PlatformFunSuite[F, Codec0]:
  import platform.*

  given [A: Arbitrary]: Arbitrary[NonEmptyList[A]] = Arbitrary(
    Gen
      .choose(1, 255)
      .flatMap(i => Gen.sequence[List[A], A](List.fill(i)(Arbitrary.arbitrary[A])))
      .map(v => NonEmptyList.fromListUnsafe(v))
  )

  def testEquality[A: Arbitrary](tpe: Type[A])(using Location, NotGiven[A <:< Option[_]]): Unit =
    test(s"Equality - ${tpe.name}"):
      given DbType = dbFixture()
      PropF.forAllF: (a: A, b: A) =>
        Operation
          .Select(
            Query.of(
              a.as(tpe) === b.as(tpe),
              a.asNullable(tpe) === b.asNullable(tpe),
              a.asNullable(tpe) === DbValue.nullV(tpe),
              DbValue.nullV(tpe) === b.asNullable(tpe),
              DbValue.nullV(tpe) === DbValue.nullV(tpe)
            )
          )
          .runOne
          .map { (r1, r2, r3, r4, r5) =>
            assertEquals(r1, a == b)
            assertEquals(r2, Some(a == b))
            assertEquals(r3, Some(false))
            assertEquals(r4, Some(false))
            assertEquals(r5, Some(true))
          }
  end testEquality

  def testOrdering[A: Arbitrary: Ordering](tpe: Type[A])(using Location): Unit = test(s"Ordering - ${tpe.name}"):
    given DbType = dbFixture()
    PropF.forAllF: (vs: NonEmptyList[A]) =>
      val vsSorted = vs.toList.sorted
      for
        r1 <- Operation.Select(Query.values(tpe)(vs.head, vs.tail*).orderBy(_.asc)).run
        r2 <- Operation.Select(Query.values(tpe)(vs.head, vs.tail*).orderBy(_.desc)).run
      yield
        assertEquals(r1, vsSorted)
        assertEquals(r2, vsSorted.reverse)
  end testOrdering

  def testNumericOperators[A: Arbitrary: Fractional: SqlNumeric](
      tpe: Type[A],
      nullableOrElse: Nullable[A] => A
  )(using Location): Unit =
    import Fractional.Implicits.*
    test(s"NumericOperators - ${tpe.name}(${if tpe == tpe.choice.notNull then "NOT NULL" else "NULL"})"):
      given DbType = dbFixture()
      PropF.forAllF: (a: A, b: A) =>
        val av = a.as(tpe)
        val bv = b.as(tpe)

        Operation
          .Select(
            Query.of(
              av + bv,
              bv + av,
              av - bv,
              bv - av,
              av * bv,
              bv * av,
              av / bv,
              bv / av,
              -av,
              -bv
            )
          )
          .runOne
          .map { (abAdd, baAdd, abSub, baSub, abMul, baMul, abDiv, baDiv, aNeg, bNeg) =>
            assertEquals(abAdd, a + b)
            assertEquals(baAdd, b + a)
            assertEquals(abSub, a - b)
            assertEquals(baSub, b - a)
            assertEquals(abMul, a * b)
            assertEquals(baMul, b * a)
            assertEquals(abDiv, a / b)
            assertEquals(baDiv, b / a)
            assertEquals(aNeg, -a)
            assertEquals(bNeg, -b)
          }

    test(s"NumericAggOperators - ${tpe.name}(${if tpe == tpe.choice.notNull then "NOT NULL" else "NULL"})"):
      given DbType = dbFixture()
      PropF.forAllF: (vs: NonEmptyList[A]) =>
        Operation
          .Select(Query.values(tpe)(vs.head, vs.tail*).mapSingleGrouped(v => (v.sum, v.avg)))
          .runOne
          .map: (sum, avg) =>
            assertEquals(nullableOrElse(sum), vs.toList.sum)
            assertEquals(
              nullableOrElse(avg),
              vs.toList.product / summon[Fractional[A]].fromInt(vs.length)
            )
  end testNumericOperators

  def testNumericNullOperators[A: Arbitrary: Fractional](
      tpe: Type[A]
  )(using Location, NotGiven[A <:< Option[_]], SqlNumeric[Option[A]]): Unit =
    val Frac: Fractional[A] = summon[Fractional[A]]
    given Fractional[Option[A]] with {
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
    testNumericOperators[Option[A]](tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A]].nullable, identity)
