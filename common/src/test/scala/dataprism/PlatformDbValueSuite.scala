package dataprism

import scala.concurrent.duration.*
import scala.concurrent.duration.Duration
import scala.util.NotGiven
import cats.data.NonEmptyList
import cats.syntax.all.*
import cats.{Apply, MonadThrow}
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.NullabilityTypeChoice
import munit.{Compare, Location}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.effect.PropF
import perspective.Id

trait PlatformDbValueSuite[F[_]: MonadThrow, Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[F, Codec0, Platform]:
  import platform.Api.*
  import platform.{AnsiTypes, name}

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

  def typeTest[A](name: String, tpe: Type[A])(body: => Any)(using Location): Unit =
    test(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(body)

  override def scalaCheckInitialSeed = "wh8YvQTetC8U3TY28hzbkGMwfuQTR2JxADe8IjdQJHL="

  override def munitIOTimeout: Duration = 10.hours

  def testEquality[A, N[_]: Apply](
      tpe: Type[N[A]],
      gen: Gen[N[A]]
  )(using N: Nullability.Aux[N[A], A, N]): Unit =
    typeTest("Equality", tpe):
      given DbType = dbFixture()
      PropF.forAllF(gen, gen): (a: N[A], b: N[A]) =>
        Select(
          Query.of(
            (
              a.as(tpe) === b.as(tpe),
              a.as(tpe).nullIf(b.as(tpe)),
              Case(a.as(tpe)).when(b.as(tpe))(DbValue.trueV).otherwise(DbValue.falseV),
              a.as(tpe).in(b.as(tpe)),
              a.as(tpe).notIn(b.as(tpe))
            )
          )
        ).runOne[F]
          .map: (r1, r2, r3, r4, r5) =>
            assertEquals(r1, a.map2(b)(_ == _))
            assertEquals(N.nullableToOption(r2), if a == b then None else N.wrapOption(a))
            assertEquals(r3, N.wrapOption(a.map2(b)(_ == _)).getOrElse(false))
            assertEquals(r4, a.map2(b)(_ == _))
            assertEquals(r5, a.map2(b)(_ != _))

    typeTest("EqualityAgg", tpe):
      given DbType = dbFixture()
      PropF.forAllF(gen, gen, Gen.listOf(gen)): (vh1: N[A], vh2: N[A], vt: List[N[A]]) =>
        val vs    = vh2 :: vt
        val vsSet = vs.map(N.wrapOption).toSet
        val vtSet = vt.map(N.wrapOption).toSet
        Select(
          Query.of(
            (
              vh1.as(tpe).in(Query.values(tpe)(vh2, vt*)),
              vh1.as(tpe).inAs(vt, tpe),
              vh1.as(tpe).notIn(Query.values(tpe)(vh2, vt*)),
              vh1.as(tpe).notInAs(vt, tpe),
              vt.foldLeft(
                (Case(vh1.as(tpe)).when(vh2.as(tpe))(0.as(AnsiTypes.integer)), 1)
              ) { case ((cse, i), v) =>
                (cse.when(v.as(tpe))(i.as(AnsiTypes.integer)), i + 1)
              }._1
                .otherwise(-1.as(AnsiTypes.integer))
            )
          )
        ).runOne[F]
          .map: (r1, r2, r3, r4, r5) =>
            def inSet(set: Set[Option[A]]): Option[Boolean] =
              N.wrapOption(vh1)
                .flatMap: h =>
                  if set.contains(Some(h)) then Some(true)
                  else if set.contains(None) then None
                  else Some(false)

            val sql = Select(
              Query.of(
                (
                  vh1.as(tpe).in(Query.values(tpe)(vh2, vt*)),
                  vh1.as(tpe).inAs(vt, tpe),
                  vh1.as(tpe).notIn(Query.values(tpe)(vh2, vt*)),
                  vh1.as(tpe).notInAs(vt, tpe),
                  vt.foldLeft(
                    (Case(vh1.as(tpe)).when(vh2.as(tpe))(0.as(AnsiTypes.integer)), 1)
                  ) { case ((cse, i), v) =>
                    (cse.when(v.as(tpe))(i.as(AnsiTypes.integer)), i + 1)
                  }._1
                    .otherwise(-1.as(AnsiTypes.integer))
                )
              )
            ).sqlAndTypes._1

            assertEquals(N.wrapOption(r1), inSet(vsSet))
            assertEquals(N.wrapOption(r2), inSet(vtSet))
            assertEquals(N.wrapOption(r3), inSet(vsSet).map(!_))
            assertEquals(N.wrapOption(r4), inSet(vtSet).map(!_))
            assertEquals(r5, if vh1 == None then -1 else vs.indexOf(vh1))
  end testEquality

  def testEqualityNotNull[A](tpe: Type[A], gen: Gen[A])(using NotGiven[A <:< Option[_]]): Unit =
    testEquality[A, Id](tpe, gen)

  def testEqualityNullable[A](tpe: Type[A], gen: Gen[A])(using NotGiven[A <:< Option[_]]): Unit =
    testEquality[A, Option](tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A]].nullable, Gen.option(gen))

  def testOrderByAscDesc[A: Ordering](tpe: Type[A], gen: Gen[A]): Unit = typeTest("OrderByAscDesc", tpe):
    given DbType = dbFixture()
    PropF.forAllF(gen, Gen.listOf(gen)): (vh: A, vt: List[A]) =>
      val vsSorted = (vh :: vt).sorted
      for
        r1 <- Select(Query.values(tpe)(vh, vt*).orderBy(_.asc)).run
        r2 <- Select(Query.values(tpe)(vh, vt*).orderBy(_.desc)).run
      yield
        assertEquals(r1, vsSorted)
        assertEquals(r2, vsSorted.reverse)
  end testOrderByAscDesc

  trait Div[A]:
    extension (a: A) def /(b: A): A
  object Div:
    given divFromFrac[A](using frac: Fractional[A]): Div[A] with
      extension (a: A) override def /(b: A): A = frac.div(a, b)

    given divFramInt[A](using int: Integral[A]): Div[A] with
      extension (a: A) override def /(b: A): A = int.quot(a, b)

  def testNumeric[A, N[_]](
      tpe: Type[N[A]],
      gen: Gen[N[A]],
      convertSum: A => SqlNumeric.SumResultOf[A]
  )(using Numeric[N[A]], Numeric[A], Div[N[A]], Div[A], Div[Option[A]], SqlNumeric[N[A]])(
      using N: Nullability.Aux[N[A], A, N]
  ): Unit =
    import Numeric.Implicits.*
    typeTest("Numeric", tpe):
      given DbType = dbFixture()
      PropF.forAllF(gen, gen): (a: N[A], b: N[A]) =>
        val av = a.as(tpe)
        val bv = b.as(tpe)

        Select(
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
        ).runOne
          .map: (abAdd, baAdd, abSub, baSub, abMul, baMul, abDiv, baDiv, aNeg, bNeg) =>
            assertEquals(abAdd, a + b)
            assertEquals(baAdd, b + a)
            assertEquals(abSub, a - b)
            assertEquals(baSub, b - a)
            assertEquals(abMul, a * b)
            assertEquals(baMul, b * a)
            assertEquals(N.nullableToOption(abDiv), if b == Numeric[N[A]].zero then None else N.wrapOption(a / b))
            assertEquals(N.nullableToOption(baDiv), if a == Numeric[N[A]].zero then None else N.wrapOption(b / a))
            assertEquals(aNeg, -a)
            assertEquals(bNeg, -b)

    typeTest("NumericAgg", tpe):
      given DbType = dbFixture()
      PropF.forAllF(gen, Gen.listOf(gen)): (vh: N[A], vt: List[N[A]]) =>
        val vs = vh :: vt
        Select(Query.values(tpe)(vh, vt*).mapSingleGrouped(v => (v.sum, v.avg))).runOne
          .map: _ =>
            ()
            // Not bothering trying to check and sum average correctly. If the queries complete that's good enough for now
  end testNumeric

  def testNumericNotNull[A: Numeric: Div: SqlNumeric](
      tpe: Type[A],
      gen: Gen[A],
      convertSum: A => SqlNumeric.SumResultOf[A]
  )(using NotGiven[A <:< Option[_]], Div[Option[A]]): Unit =
    testNumeric[A, Id](tpe, gen, convertSum)

  def testNumericNullable[A: Numeric: Div](
      tpe: Type[A],
      gen: Gen[A],
      convertSum: A => SqlNumeric.SumResultOf[A]
  )(using NotGiven[A <:< Option[_]], SqlNumeric[Option[A]], Numeric[Option[A]], Div[Option[A]]): Unit =
    testNumeric[A, Option](tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A]].nullable, Gen.option(gen), convertSum)

  def testOrdered[A, N[_]: Apply](
      tpe: Type[N[A]],
      gen: Gen[N[A]]
  )(
      using Ordering[A],
      Ordering[N[A]],
      SqlOrdered[N[A]] { val n: Nullability.Aux[N[A], A, N] }
  )(using N: Nullability.Aux[N[A], A, N]): Unit =
    import Ordering.Implicits.*
    typeTest("Ordered", tpe):
      given DbType = dbFixture()
      PropF.forAllF(gen, gen): (a: N[A], b: N[A]) =>
        val av = a.as(tpe)
        val bv = b.as(tpe)

        Select(Query.of(av < bv, bv < av, av <= bv, bv <= av, av >= bv, bv >= av, av > bv, bv > av))
          .runOne[F]
          .map: (abLt, baLt, abLe, baLe, abGe, baGe, abGt, baGt) =>
            assertEquals(abLt, a.map2(b)(_ < _))
            assertEquals(baLt, b.map2(a)(_ < _))
            assertEquals(abLe, a.map2(b)(_ <= _))
            assertEquals(baLe, b.map2(a)(_ <= _))
            assertEquals(abGe, a.map2(b)(_ >= _))
            assertEquals(baGe, b.map2(a)(_ >= _))
            assertEquals(abGt, a.map2(b)(_ > _))
            assertEquals(baGt, b.map2(a)(_ > _))

    typeTest("OrderedList", tpe):
      given DbType = dbFixture()
      PropF.forAllF(gen, Gen.listOf(gen)): (vh: N[A], vt: List[N[A]]) =>
        val vs  = NonEmptyList.of(vh, vt*)
        val vsv = vs.toList.map(_.as(tpe))

        for
          maxMin        <- Select(Query.values(tpe)(vh, vt*).mapSingleGrouped(v => (v.max, v.min))).runOne[F]
          greatestLeast <- Select(Query.of((vsv.head.greatest(vsv.tail*), vsv.head.least(vsv.tail*)))).runOne[F]
        yield
          val (max, min)        = maxMin
          val (greatest, least) = greatestLeast
          val someVs            = vs.toList.flatMap(N.wrapOption(_))
          assertEquals(N.nullableToOption(max), someVs.maxOption)
          assertEquals(N.nullableToOption(min), someVs.minOption)
          assertEquals(N.wrapOption(greatest), if someVs.length != vs.length then None else N.wrapOption(vs.toList.max))
          assertEquals(N.wrapOption(least), if someVs.length != vs.length then None else N.wrapOption(vs.toList.min))
  end testOrdered

  def testOrderedNotNull[A: Ordering: cats.Order](
      tpe: Type[A],
      gen: Gen[A],
  )(using SqlOrdered[A] { val n: Nullability.Aux[A, A, Id] }): Unit =
    testOrdered[A, Id](tpe, gen)

  def testOrderedNullable[A: Ordering](tpe: Type[A], gen: Gen[A])(
      using NotGiven[A <:< Option[_]],
      cats.Order[Option[A]],
      SqlOrdered[Option[A]] { val n: Nullability.Aux[Option[A], A, Option] }
  ): Unit =
    testOrdered[A, Option](tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A]].nullable, Gen.option(gen))

  typeTest("BooleanOps", AnsiTypes.boolean):
    given DbType = dbFixture()
    val boolean  = AnsiTypes.boolean
    PropF.forAllF: (a: Boolean, b: Boolean, an: Option[Boolean], bn: Option[Boolean]) =>
      val av  = a.as(boolean)
      val bv  = b.as(boolean)
      val anv = an.as(boolean.nullable)
      val bnv = bn.as(boolean.nullable)
      Select(
        Query.of(
          (
            av || bv,
            bv || av,
            av && bv,
            bv && av,
            !av,
            !bv,
            anv || bnv,
            bnv || anv,
            anv && bnv,
            bnv && anv,
            !anv,
            !bnv
          )
        )
      ).runOne[F]
        .map: (abOr, baOr, abAnd, baAnd, aNot, bNot, abnOr, banOr, abnAnd, banAnd, anNot, bnNot) =>
          assertEquals(abOr, a || b)
          assertEquals(baOr, b || a)
          assertEquals(abAnd, a && b)
          assertEquals(baAnd, b && a)
          assertEquals(aNot, !a)
          assertEquals(bNot, !b)

          assertEquals(abnOr, Option.when(an.contains(true) || bn.contains(true))(true).orElse(an.map2(bn)(_ || _)))
          assertEquals(banOr, Option.when(an.contains(true) || bn.contains(true))(true).orElse(bn.map2(an)(_ || _)))
          assertEquals(abnAnd, Option.when(an.contains(false) || bn.contains(false))(false).orElse(an.map2(bn)(_ && _)))
          assertEquals(banAnd, Option.when(an.contains(false) || bn.contains(false))(false).orElse(bn.map2(an)(_ && _)))
          assertEquals(anNot, an.map(!_))
          assertEquals(bnNot, bn.map(!_))

  def testNullOps[A](t: Type[Option[A]], gen: Gen[A]): Unit = typeTest("NullOps", t):
    given DbType = dbFixture()
    val optGen = Gen.option(gen)
    PropF.forAllF(optGen, optGen, optGen): (o1: Option[A], o2: Option[A], o3: Option[A]) =>
      val v1 = o1.as(t)
      val v2 = o2.as(t)
      val v3 = o3.as(t)

      Select(
        Query.of(
          v1,
          v1.map(a => a),
          v1.flatMap(_ => v2),
          v1.filter(_ => DbValue.trueV),
          v1.filter(_ => DbValue.falseV),
          (v1, v2, v3).mapNullableN((n1, _, _) => n1),
          (v1, v2, v3).mapNullableN((_, n2, _) => n2),
          (v1, v2, v3).mapNullableN((_, _, n3) => n3)
        )
      ).runOne[F]
        .map: (r1, r2, r3, r4, r5, r6, r7, r8) =>
          assertEquals(r1, o1)
          assertEquals(r2, o1)
          assertEquals(r3, o1.flatMap(_ => o2))
          assertEquals(r4, o1)
          assertEquals(r5, None)
          assertEquals(r6, (o1, o2, o3).mapN((n1, _, _) => n1))
          assertEquals(r7, (o1, o2, o3).mapN((_, n2, _) => n2))
          assertEquals(r8, (o1, o2, o3).mapN((_, _, n3) => n3))

  test("CaseBoolean"):
    given DbType = dbFixture()
    val boolean  = AnsiTypes.boolean
    val int      = AnsiTypes.integer
    PropF.forAllF: (b1: Boolean, b2: Boolean, b3: Boolean, i1: Int, i2: Int, i3: Int, i4: Int) =>
      Select(
        Query.of(
          Case
            .when(b1.as(boolean))(i1.as(int))
            .when(b2.as(boolean))(i2.as(int))
            .when(b3.as(boolean))(i3.as(int))
            .otherwise(i4.as(int))
        )
      ).runOne[F]
        .map: r =>
          assertEquals(r, if b1 then i1 else if b2 then i2 else if b3 then i3 else i4)

  private val intGen = Gen.choose(-10000, 10000)
  
  testEqualityNotNull(AnsiTypes.integer, intGen)
  testEqualityNullable(AnsiTypes.integer, intGen)
  testOrderByAscDesc(AnsiTypes.integer, intGen)
  testNumericNotNull(AnsiTypes.integer, intGen, _.toLong)
  testNumericNullable(AnsiTypes.integer, intGen, _.toLong)
  testOrderedNotNull(AnsiTypes.integer, intGen)
  testOrderedNullable(AnsiTypes.integer, intGen)
  testNullOps(AnsiTypes.integer.nullable, intGen)
  
  private val doubleGen = Gen.choose(-10000D, 10000D)

  testEqualityNotNull(AnsiTypes.doublePrecision, doubleGen)
  testEqualityNullable(AnsiTypes.doublePrecision, doubleGen)
  testOrderByAscDesc(AnsiTypes.doublePrecision, doubleGen)
  testNumericNotNull(AnsiTypes.doublePrecision, doubleGen, identity)
  testNumericNullable(AnsiTypes.doublePrecision, doubleGen, identity)
  testOrderedNotNull(AnsiTypes.doublePrecision, doubleGen)
  testOrderedNullable(AnsiTypes.doublePrecision, doubleGen)
  testNullOps(AnsiTypes.doublePrecision.nullable, doubleGen)
