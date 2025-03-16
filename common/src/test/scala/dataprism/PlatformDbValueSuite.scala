package dataprism

import scala.util.NotGiven

import cats.data.NonEmptyList
import cats.data.Validated.Valid
import cats.effect.IO
import cats.syntax.all.*
import cats.{Apply, Show}
import dataprism.PlatformFunSuite.DbToTest
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.platform.sql.value.SqlBitwiseOps
import dataprism.sql.{NullabilityTypeChoice, SqlNull}
import org.scalacheck.cats.implicits.*
import org.scalacheck.{Arbitrary, Gen}
import perspective.Id
import weaver.{Expectations, Log, SourceLocation}

trait PlatformDbValueSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[Codec0, Platform],
      WithOptionalInstances:
  import platform.Api.*
  import platform.{AnsiTypes, name}

  def typeTest[A](name: String, tpe: Type[A])(run: DbType ?=> IO[Expectations])(using SourceLocation): Unit =
    dbTest(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(run)

  def typeLogTest[A](name: String, tpe: Type[A])(run: DbType ?=> Log[IO] => IO[Expectations])(
      using SourceLocation
  ): Unit =
    dbLogTest(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(run)

  val configuredForall: PartiallyAppliedForall = forall

  def testEquality[A, N[_]: Apply](
      tpe: Type[N[A]],
      gen: Gen[N[A]]
  )(using N: Nullability.Aux[N[A], A, N])(using Show[N[A]]): Unit =
    typeTest("Equality", tpe):
      configuredForall((gen, gen).tupled): (a: N[A], b: N[A]) =>
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
            Seq(
              expect.same(a.map2(b)(_ == _), r1),
              expect.same(if a == b then SqlNull else a, r2),
              expect.same(N.wrapOption(a.map2(b)(_ == _)).getOrElse(false), r3),
              expect.same(a.map2(b)(_ == _), r4),
              expect.same(a.map2(b)(_ != _), r5)
            ).combineAll

    typeLogTest("EqualityAgg", tpe): log =>
      configuredForall((gen, gen, Gen.listOf(gen)).tupled): (vh1: N[A], vh2: N[A], vt: List[N[A]]) =>
        val vs    = vh2 :: vt
        val vsSet = vs.map(N.wrapOption).toSet
        val vtSet = vt.map(N.wrapOption).toSet

        val query = Select(
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
        )

        def inSet(set: Set[Option[A]]): Option[Boolean] =
          N.wrapOption(vh1)
            .flatMap: h =>
              if set.contains(Some(h)) then Some(true)
              else if set.contains(None) then None
              else Some(false)

        for
          _ <- ignore("MySQL is weird with equality agg operators").whenA(
            dbToTest == DbToTest.MySql8 || dbToTest == DbToTest.MySql57
          )
          res <- query.runOne[F]
          (r1, r2, r3, r4, r5) = res
        yield Seq(
          expect.same(inSet(vsSet), N.wrapOption(r1)),
          expect.same(inSet(vtSet), N.wrapOption(r2)),
          expect.same(inSet(vsSet).map(!_), N.wrapOption(r3)),
          expect.same(inSet(vtSet).map(!_), N.wrapOption(r4)),
          expect.same(if vh1 == SqlNull then -1 else vs.indexOf(vh1), r5)
        ).combineAll
  end testEquality

  def testEqualityNotNull[A: Show](tpe: Type[A], gen: Gen[A])(using NotGiven[SqlNull <:< A]): Unit =
    testEquality[A, Id](tpe, gen)

  def testEqualityNullable[A: Show](tpe: Type[A], gen: Gen[A])(using NotGiven[SqlNull <:< A]): Unit =
    import dataprism.sql.sqlNullSyntax.given
    testEquality[A, Nullable](
      tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A, tpe.Dimension]].nullable,
      sqlNullGen(gen)
    )

  def testOrderByAscDesc[A: Ordering: Show](tpe: Type[A], gen: Gen[A]): Unit = typeTest("OrderByAscDesc", tpe):
    configuredForall((gen, Gen.listOf(gen)).tupled): (vh: A, vt: List[A]) =>
      val vsSorted = (vh :: vt).sorted
      for
        r1 <- Select(Query.values(tpe)(vh, vt*).orderBy(_.asc)).run
        r2 <- Select(Query.values(tpe)(vh, vt*).orderBy(_.desc)).run
      yield expect.same(vsSorted, r1) && expect.same(vsSorted.reverse, r2)
  end testOrderByAscDesc

  trait Div[A]:
    extension (a: A) def /(b: A): A
  object Div:
    given divFromFrac[A](using frac: Fractional[A]): Div[A] with
      extension (a: A) override def /(b: A): A = frac.div(a, b)

    given divFramInt[A](using int: Integral[A]): Div[A] with
      extension (a: A) override def /(b: A): A = int.quot(a, b)

  def testNumeric[A, N[_], SumResult, AvgResult](
      tpe: Type[N[A]],
      gen: Gen[N[A]],
      delta: N[A]
  )(
      using Numeric[N[A]],
      Numeric[A],
      Div[N[A]],
      Div[A],
      Div[Nullable[A]],
      SqlNumericSumAverage[N[A], SumResult, AvgResult],
      Show[N[A]]
  )(
      using N: Nullability.Aux[N[A], A, N]
  ): Unit =
    import Numeric.Implicits.*
    import Ordering.Implicits.*
    typeTest("Numeric", tpe):
      configuredForall((gen, gen).tupled): (a: N[A], b: N[A]) =>
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
            Seq(
              expect(((a + b) - abAdd).abs <= delta),
              expect(((b + a) - baAdd).abs <= delta),
              expect(((a - b) - abSub).abs <= delta),
              expect(((b - a) - baSub).abs <= delta),
              expect(((a * b) - abMul).abs <= delta),
              expect(((b * a) - baMul).abs <= delta),
              expect {
                import dataprism.sql.sqlNullSyntax.*

                val lhs            = if b == Numeric[N[A]].zero then None else N.wrapOption(a / b)
                val rhs: Option[A] = N.nullableToOption(abDiv)

                (lhs, rhs, N.wrapOption(delta))
                  .mapN((a, b, del) => (a - b).abs <= del)
                  .getOrElse(lhs.isEmpty && rhs.isEmpty)
              },
              expect {
                import dataprism.sql.sqlNullSyntax.*

                val lhs            = if a == Numeric[N[A]].zero then None else N.wrapOption(b / a)
                val rhs: Option[A] = N.nullableToOption(baDiv)

                (lhs, rhs, N.wrapOption(delta))
                  .mapN((a, b, del) => (a - b).abs <= del)
                  .getOrElse(lhs.isEmpty && rhs.isEmpty)
              },
              expect.same(-a, aNeg),
              expect.same(-b, bNeg)
            ).combineAll

    typeTest("NumericAgg", tpe):
      configuredForall((gen, Gen.listOf(gen)).tupled): (vh: N[A], vt: List[N[A]]) =>
        val vs = vh :: vt
        Select(Query.values(tpe)(vh, vt*).mapSingleGrouped(v => (v.sum, v.avg))).runOne
          .map: _ =>
            // Not bothering trying to check and sum average correctly. If the queries complete that's good enough for now
            Expectations(Valid(()))
  end testNumeric

  def testNumericNotNull[A: Numeric: Div: Show, SumResult, AvgResult](
      tpe: Type[A],
      gen: Gen[A],
      delta: A
  )(using NotGiven[SqlNull <:< A], Div[Nullable[A]], SqlNumericSumAverage[A, SumResult, AvgResult]): Unit =
    testNumeric[A, Id, SumResult, AvgResult](tpe, gen, delta)

  def testNumericNullable[A: Numeric: Div: Show, SumResult, AvgResult](
      tpe: Type[A],
      gen: Gen[A],
      delta: A
  )(
      using NotGiven[SqlNull <:< A],
      Numeric[Nullable[A]],
      Div[Nullable[A]],
      SqlNumericSumAverage[Nullable[A], SumResult, AvgResult]
  ): Unit =
    testNumeric[A, Nullable, SumResult, AvgResult](
      tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A, tpe.Dimension]].nullable,
      sqlNullGen(gen),
      delta
    )

  def leastGreatestBubbleNulls: Boolean

  def testOrdered[A, N[_]: Apply](
      tpe: Type[N[A]],
      gen: Gen[N[A]]
  )(
      using Ordering[A],
      Ordering[N[A]],
      SqlOrdered[N[A]],
      Show[N[A]]
  )(using N: Nullability.Aux[N[A], A, N]): Unit =
    import Ordering.Implicits.*
    typeTest("Ordered", tpe):
      configuredForall((gen, gen).tupled): (a: N[A], b: N[A]) =>
        val av = a.as(tpe)
        val bv = b.as(tpe)

        Select(Query.of(av < bv, bv < av, av <= bv, bv <= av, av >= bv, bv >= av, av > bv, bv > av))
          .runOne[F]
          .map: (abLt, baLt, abLe, baLe, abGe, baGe, abGt, baGt) =>
            Seq(
              expect.same(a.map2(b)(_ < _), abLt),
              expect.same(b.map2(a)(_ < _), baLt),
              expect.same(a.map2(b)(_ <= _), abLe),
              expect.same(b.map2(a)(_ <= _), baLe),
              expect.same(a.map2(b)(_ >= _), abGe),
              expect.same(b.map2(a)(_ >= _), baGe),
              expect.same(a.map2(b)(_ > _), abGt),
              expect.same(b.map2(a)(_ > _), baGt)
            ).combineAll

    typeLogTest("OrderedList", tpe): log =>
      configuredForall((gen, Gen.listOf(gen)).tupled): (vh: N[A], vt: List[N[A]]) =>
        val vs  = NonEmptyList.of(vh, vt*)
        val vsv = vs.toList.map(_.as(tpe))

        for
          maxMin        <- Select(Query.values(tpe)(vh, vt*).mapSingleGrouped(v => (v.max, v.min))).runOne[F]
          greatestLeast <- Select(Query.of((vsv.head.greatest(vsv.tail*), vsv.head.least(vsv.tail*)))).runOne[F]
          _             <- log.debug(s"Using list ${vs.toList}")
          _             <- log.debug(s"Got max=${maxMin._1} min=${maxMin._2}")
          _             <- log.debug(s"Got greatest=${greatestLeast._1} least=${greatestLeast._2}")
        yield
          import dataprism.sql.sqlNullSyntax.*

          val (max, min)        = maxMin
          val (greatest, least) = greatestLeast
          val someVs            = vs.toList.flatMap(N.wrapOption(_))
          Seq(
            expect.same(someVs.maxOption, max.toOption),
            expect.same(someVs.minOption, min.toOption),
            expect.same(
              if leastGreatestBubbleNulls && someVs.length != vs.length then None
              else N.wrapOption(vs.toList.filter(_ != SqlNull).max),
              N.wrapOption(greatest)
            ),
            expect.same(
              if leastGreatestBubbleNulls && someVs.length != vs.length then None
              else N.wrapOption(vs.toList.filter(_ != SqlNull).min),
              N.wrapOption(least)
            )
          ).combineAll
  end testOrdered

  def testOrderedNotNull[A: Ordering: cats.Order: Show](
      tpe: Type[A],
      gen: Gen[A]
  )(using SqlOrdered[A]): Unit =
    testOrdered[A, Id](tpe, gen)

  def testOrderedNullable[A: Ordering: Show](tpe: Type[A], gen: Gen[A])(
      using NotGiven[SqlNull <:< A],
      cats.Order[Nullable[A]],
      SqlOrdered[Nullable[A]]
  ): Unit =
    import dataprism.sql.sqlNullSyntax.given
    testOrdered[A, Nullable](
      tpe.choice.asInstanceOf[NullabilityTypeChoice[Codec, A, tpe.Dimension]].nullable,
      sqlNullGen(gen)
    )

  typeTest("BooleanOps", AnsiTypes.boolean):
    val boolean = AnsiTypes.boolean
    configuredForall: (a: Boolean, b: Boolean, an: Boolean | SqlNull, bn: Boolean | SqlNull) =>
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
          import dataprism.sql.sqlNullSyntax.{*, given}
          import cats.syntax.all.*

          def sqlNullWhen[A](cond: Boolean)(v: A) = if cond then v else SqlNull

          Seq(
            expect.same(a || b, abOr),
            expect.same(b || a, baOr),
            expect.same(a && b, abAnd),
            expect.same(b && a, baAnd),
            expect.same(!a, aNot),
            expect.same(!b, bNot),
            //
            expect.same(sqlNullWhen(an.contains(true) || bn.contains(true))(true).orElse(an.map2(bn)(_ || _)), abnOr),
            expect.same(sqlNullWhen(an.contains(true) || bn.contains(true))(true).orElse(bn.map2(an)(_ || _)), banOr),
            expect.same(
              sqlNullWhen(an.contains(false) || bn.contains(false))(false).orElse(an.map2(bn)(_ && _)),
              abnAnd
            ),
            expect.same(
              sqlNullWhen(an.contains(false) || bn.contains(false))(false).orElse(bn.map2(an)(_ && _)),
              banAnd
            ),
            expect.same(an.map(!_), anNot),
            expect.same(bn.map(!_), bnNot)
          ).combineAll

  def testNullOps[A: Show](t: Type[A | SqlNull], gen: Gen[A]): Unit = typeTest("NullOps", t):
    val optGen = sqlNullGen(gen)
    configuredForall((optGen, optGen, optGen).tupled): (o1: Nullable[A], o2: Nullable[A], o3: Nullable[A]) =>
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
          import dataprism.sql.sqlNullSyntax.{*, given}
          Seq(
            expect.same(o1, r1),
            expect.same(o1, r2),
            expect.same(o1.flatMap(_ => o2), r3),
            expect.same(o1, r4),
            expect.same(SqlNull, r5),
            expect.same((o1, o2, o3).mapN((n1, _, _) => n1), r6),
            expect.same((o1, o2, o3).mapN((_, n2, _) => n2), r7),
            expect.same((o1, o2, o3).mapN((_, _, n3) => n3), r8)
          ).combineAll

  dbTest("CaseBoolean"):
    val boolean = AnsiTypes.boolean
    val int     = AnsiTypes.integer
    configuredForall: (b1: Boolean, b2: Boolean, b3: Boolean, i1: Int, i2: Int, i3: Int, i4: Int) =>
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
          expect.same(if b1 then i1 else if b2 then i2 else if b3 then i3 else i4, r)

  def testBitwiseOps[A, N[_]](
      bitwisePlatform: Platform & SqlBitwiseOps,
      tpe: Type[N[A]],
      gen: Gen[N[A]],
      toInt: A => Int
  )(
      using show: Show[N[A]],
      bw: bitwisePlatform.Api.SqlBitwise[N[A]],
      N: bitwisePlatform.Nullability.Aux[N[A], A, N],
      bool: algebra.lattice.Bool[A]
  ): Unit = typeTest("BitwiseOps", tpe):
    import bitwisePlatform.Api.*
    configuredForall((gen, gen).tupled): (a: N[A], b: N[A]) =>
      val av = a.as(tpe)
      val bv = b.as(tpe)

      Select(
        Query.of(
          (
            av & bv,
            av | bv,
            av ^ bv,
            ~av,
            ~bv
          )
        )
      ).runOne[F]
        .map: (and, or, xor, aNot, bNot) =>
          val ao = N.wrapOption(a)
          val bo = N.wrapOption(b)

          expect.all(
            N.wrapOption(and) == (ao, bo).mapN(bool.and),
            N.wrapOption(or) == (ao, bo).mapN(bool.or),
            N.wrapOption(xor) == (ao, bo).mapN(bool.xor),
            N.wrapOption(aNot) == ao.map(bool.complement),
            N.wrapOption(bNot) == bo.map(bool.complement)
          )

  private val intGen = Gen.choose(-10000, 10000)

  testEqualityNotNull(AnsiTypes.integer, intGen)
  testEqualityNullable(AnsiTypes.integer, intGen)
  testOrderByAscDesc(AnsiTypes.integer, intGen)
  testNumericNotNull(AnsiTypes.integer, intGen, 0)
  testNumericNullable(AnsiTypes.integer, intGen, 0)
  testOrderedNotNull(AnsiTypes.integer, intGen)
  testOrderedNullable(AnsiTypes.integer, intGen)
  testNullOps(AnsiTypes.integer.nullable, intGen)

  private val doubleGen = Gen.choose(-10000D, 10000D)

  testEqualityNotNull(AnsiTypes.doublePrecision, doubleGen)
  testEqualityNullable(AnsiTypes.doublePrecision, doubleGen)
  testOrderByAscDesc(AnsiTypes.doublePrecision, doubleGen)
  testNumericNotNull(AnsiTypes.doublePrecision, doubleGen, 1E-5)
  testNumericNullable(AnsiTypes.doublePrecision, doubleGen, 1E-5)
  testOrderedNotNull(AnsiTypes.doublePrecision, doubleGen)
  testOrderedNullable(AnsiTypes.doublePrecision, doubleGen)
  testNullOps(AnsiTypes.doublePrecision.nullable, doubleGen)
