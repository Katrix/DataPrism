package dataprism

import cats.MonadThrow
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import munit.FunSuite

trait PlatformQuerySuite[F[_]: MonadThrow, Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[F, Codec0, Platform] {
  import platform.AnsiTypes.*
  import platform.Api.*

  test("Nested"):
    given DbType = dbFixture()
    Select(Query.of(5.as(integer)).nested).runOne.map: r =>
      assertEquals(r, 5)

  test("Where"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).nested.where(_ >= 3.as(integer))).run.map: r =>
      assertEquals(r, Seq(5, 3, 5))

  test("Map"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).nested.map(_ + 2.as(integer))).run.map: r =>
      assertEquals(r, Seq(7, 5, 7, 4))

  test("OrderBy"):
    given DbType = dbFixture()
    Select(
      Query
        .values((integer.forgetNNA, defaultStringType.forgetNNA))((5, "foo"), (3, "bar"), (6, "baz"), (2, "quox"))
        .nested
        .orderBy(_._1.asc)
    ).run.map: r =>
      assertEquals(r, Seq((2, "quox"), (3, "bar"), (5, "foo"), (6, "baz")))

  test("GroupBy"):
    given DbType = dbFixture()
    Select(
      Query
        .values((integer.forgetNNA, integer.forgetNNA))((5, 3), (3, 3), (5, 1), (2, 5))
        .nested
        .groupMap(_._1)((v, t) => (v, t._2.sum))
        .orderBy(_._1.asc)
    ).run.map: r =>
      assertEquals(r, Seq((5, Some(4)), (3, Some(3)), (2, Some(5))))

  test("GroupByHaving"):
    given DbType = dbFixture()
    Select(
      Query
        .values((integer.forgetNNA, integer.forgetNNA))((5, 3), (3, 3), (5, 1), (2, 5))
        .nested
        .groupMap(_._1)((v, t) => (v, t._2.sum))
        .having(_._2.map(_ > 3.as(integer)).getOrElse(DbValue.falseV))
        .orderBy(_._1.asc)
    ).run.map: r =>
      assertEquals(r, Seq((5, Some(4)), (2, Some(5))))

  test("Distinct"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2)).run.map: r =>
      assertEquals(r, Seq(5, 3, 2))

  test("Limit"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).limit(3)).run.map: r =>
      assertEquals(r, Seq(5, 3, 5))

  test("Offset"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).limit(3)).run.map: r =>
      assertEquals(r, Seq(3, 5, 2))

  test("LimitOffset"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).offset(1).limit(2)).run.map: r =>
      assertEquals(r, Seq(3, 5))

  test("Union"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3).union(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      assertEquals(r, Seq(5, 3, 2))

  test("UnionAll"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3).unionAll(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      assertEquals(r, Seq(5, 3, 5, 2))

  test("Intersect"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3).intersect(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      assertEquals(r, Seq(5))

  test("IntersectAll"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 3).intersectAll(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      assertEquals(r, Seq(5, 5))

  test("Except"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 4, 5, 3).except(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      assertEquals(r, Seq(4, 3))

  test("ExceptAll"):
    given DbType = dbFixture()
    Select(Query.values(integer.forgetNNA)(5, 4, 5, 3).exceptAll(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      assertEquals(r, Seq(4, 5, 3))

  test("Size"):
    given DbType = dbFixture()
    Select(Query.of(Query.values(integer.forgetNNA)(5, 3, 5, 2).size)).runOne.map: r =>
      assertEquals(r, 4L)

  def doTestFlatmapLateral()(using platform.LateralJoinCapability): Unit = test("FlatMap/LateralJoin"):
    given DbType = dbFixture()
    Select(
      Query.values(integer.forgetNNA)(5, 3).flatMap(v => Query.values(integer.forgetNNA)(5, 3).map(_ => v))
    ).run.map: r =>
      assertEquals(r, Seq(5, 5, 3, 3))
}
