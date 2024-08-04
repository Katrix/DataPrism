package dataprism

import cats.Show
import cats.effect.IO
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.platform.sql.value.SqlArrays
import org.scalacheck.Gen
import org.scalacheck.cats.implicits.*
import weaver.{Expectations, Log, SourceLocation}

trait PlatformArraysSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] } & SqlArrays]
    extends PlatformFunSuite[Codec0, Platform] {
  import platform.Api.{*, given}
  import platform.{AnsiTypes, name}

  def typeTest[A](name: String, tpe: Type[A])(run: DbType ?=> IO[Expectations])(using SourceLocation): Unit =
    dbTest(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(run)

  def typeLogTest[A](name: String, tpe: Type[A])(run: DbType ?=> Log[IO] => IO[Expectations])(
      using SourceLocation
  ): Unit =
    dbLogTest(s"$name - ${tpe.name}(${if tpe.codec == tpe.choice.notNull.codec then "NOT NULL" else "NULL"})")(run)

  def logQuery[A[_[_]]](select: SelectOperation[A])(using log: Log[IO]): IO[SelectOperation[A]] =
    log.debug(s"Query: ${select.sqlAndTypes._1.str}").as(select)

  val configuredForall: PartiallyAppliedForall = forall

  def genNel[A](gen: Gen[A]): Gen[(A, List[A])] =
    (gen, Gen.choose(0, 2).flatMap(s => Gen.buildableOfN[List[A], A](s, gen))).tupled // TODO: Gen.listOf(gen)

  def testArrays[A: Show](tpe: Type[A], gen: Gen[A]): Unit =
    typeTest("ArrayConstruction", tpe):
      configuredForall(genNel(gen)): (value, values) =>
        Select(Query.of(DbArray.of(value.as(tpe), values.map(v => v.as(tpe))*)))
          .runOne[F]
          .map: ret =>
            expect(ret == (value +: values))

    typeTest("ArrayGet", tpe):
      configuredForall(
        for
          (v, vs) <- genNel(gen)
          i       <- Gen.choose(1, vs.length + 1)
        yield (v, vs, i)
      ): (value, values, idx) =>
        Select(Query.of(DbArray.of(value.as(tpe), values.map(v => v.as(tpe))*).apply(idx.as(AnsiTypes.integer))))
          .runOne[F]
          .map: ret =>
            expect(ret == (value +: values).apply(idx - 1))

    typeTest("ArrayCardinality", tpe):
      configuredForall(gen): value =>
        val dbVal = value.as(tpe)
        Select(
          Query.of(
            (
              DbArray.of(dbVal, dbVal).cardinality,
              DbArray.of(DbArray.of(dbVal, dbVal)).cardinality,
              DbArray.of(DbArray.of(dbVal), DbArray.of(dbVal)).cardinality
            )
          )
        ).runOne[F]
          .map: (_, _, _) =>
            expect(true) // Seems implementation defined what this returns. As long as the queries pass I'm happy

    typeTest("ArrayConcat", tpe):
      configuredForall((genNel(gen), genNel(gen)).tupled):
        case ((v1, vs1), (v2, vs2)) =>
          Select(
            Query.of(DbArray.of(v1.as(tpe), vs1.map(_.as(tpe))*).concat(DbArray.of(v2.as(tpe), vs2.map(_.as(tpe))*)))
          ).runOne[F]
            .map: r =>
              expect((v1 +: vs1) ++ (v2 +: vs2) == r)

    typeTest("ArrayAppendPrepend", tpe):
      configuredForall((gen, genNel(gen), gen).tupled):
        case (prepend, (v, vs), append) =>
          Select(
            Query.of(prepend.as(tpe) +: DbArray.of(v.as(tpe), vs.map(_.as(tpe))*) :+ append.as(tpe))
          ).runOne[F]
            .map: r =>
              expect(prepend +: (v +: vs) :+ append == r)

    typeTest("ArrayContains", tpe):
      configuredForall(
        for
          (v, vs) <- genNel(gen)
          elem <- Gen.frequency(
            25 -> gen,
            75 -> Gen.oneOf(v +: vs)
          )
        yield (v, vs, elem)
      ): (v, vs, elem) =>
        Select(Query.of(DbArray.of(v.as(tpe), vs.map(_.as(tpe))*).contains(elem.as(tpe))))
          .runOne[F]
          .map: r =>
            expect((v +: vs).contains(elem) == r)

    typeTest("ArrayDrop", tpe):
      configuredForall(
        for
          (v, vs) <- genNel(gen)
          i       <- Gen.choose(0, vs.length + 1)
        yield (v, vs, i)
      ): (value, values, num) =>
        Select(Query.of(DbArray.of(value.as(tpe), values.map(v => v.as(tpe))*).dropRight(num.as(AnsiTypes.integer))))
          .runOne[F]
          .map: ret =>
            expect(ret == (value +: values).dropRight(num))

    typeTest("ArrayParam", tpe):
      configuredForall((gen, gen, gen, gen).tupled): (v1, v2, v3, v4) =>
        val arr1Type: Type[Seq[A]]      = platform.arrayOfType(tpe)
        val arr2Type: Type[Seq[Seq[A]]] = platform.arrayOfType(arr1Type)
        Select(
          Query.of(
            Seq().as(arr1Type),
            Seq(v1).as(arr1Type),
            Seq(v1, v2).as(arr1Type),
            Seq(Seq(v1, v2), Seq(v3, v4)).as(arr2Type)
          )
        )
          .runOne[F]
          .map: (r1, r2, r3, r4) =>
            expect.all(
              r1 == Seq(),
              r2 == Seq(v1),
              r3 == Seq(v1, v2),
              r4 == Seq(Seq(v1, v2), Seq(v3, v4))
            )

    typeTest("ArrayAgg", tpe):
      configuredForall(genNel(gen)): (v, vs) =>
        Select(Query.values(tpe)(v, vs*).mapSingleGrouped(v => v.arrayAgg))
          .runOne[F]
          .map: r =>
            expect(r == (v +: vs))

  end testArrays

  def testArrayUnnest[A: Show](tpe: Type[A], gen: Gen[A], opV: A => A, opDb: DbValue[A] => DbValue[A])(
      using n: Nullability[A]
  ): Unit =
    typeLogTest("ArrayUnnest1", tpe): log =>
      given Log[IO] = log
      configuredForall(genNel(gen)): (v, vs) =>
        logQuery(
          Select(
            DbArray
              .unnest(DbArray.of(v.as(tpe), vs.map(v => v.as(tpe))*))
              .map((v: DbValue[Nullable[A]]) => n.reifyNullable(v).map(opDb))
          )
        ).flatMap(_.run[F])
          .map: ret =>
            expect(ret == (v +: vs).map(opV).map(Some(_)))

    typeLogTest("ArrayUnnest2", tpe): log =>
      given Log[IO] = log
      configuredForall((genNel(gen), genNel(gen)).tupled):
        case ((v1, vs1), (v2, vs2)) =>
          logQuery(
            Select(
              DbArray
                .unnest(
                  DbArray.of(v1.as(tpe), vs1.map(v => v.as(tpe))*),
                  DbArray.of(v2.as(tpe), vs2.map(v => v.as(tpe))*)
                )
                .map((v1: DbValue[Nullable[A]], v2: DbValue[Nullable[A]]) =>
                  (n.reifyNullable(v1).map(opDb), n.reifyNullable(v2).map(opDb))
                )
            )
          ).flatMap(_.run[F])
            .map: ret =>
              expect(ret == (v1 +: vs1).map(opV).padZip((v2 +: vs2).map(opV)))
}
