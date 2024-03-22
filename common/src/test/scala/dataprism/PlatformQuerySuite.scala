package dataprism

import dataprism.platform.sql.SqlQueryPlatform

trait PlatformQuerySuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[Codec0, Platform] {
  import platform.AnsiTypes.*
  import platform.Api.*

  dbTest("NonNested"):
    Select(Query.of(5.as(integer))).runOne.map: r =>
      expect.same(5, r)

  dbTest("Nested"):
    Select(Query.of(5.as(integer)).nested).runOne.map: r =>
      expect.same(5, r)

  dbTest("Where"):
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).nested.where(_ >= 3.as(integer))).run.map: r =>
      expect.same(Set(5, 3, 5), r.toSet)

  dbTest("Map"):
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).nested.map(_ + 2.as(integer))).run.map: r =>
      expect.same(Set(7, 5, 7, 4), r.toSet)

  dbTest("OrderBy"):
    Select(
      Query
        .values((integer.forgetNNA, defaultStringType.forgetNNA))((5, "foo"), (3, "bar"), (6, "baz"), (2, "quox"))
        .nested
        .orderBy(_._1.asc)
    ).run.map: r =>
      expect.same(Seq((2, "quox"), (3, "bar"), (5, "foo"), (6, "baz")), r)

  dbTest("GroupBy"):
    Select(
      Query
        .values((integer.forgetNNA, integer.forgetNNA))((5, 3), (3, 3), (5, 1), (2, 5))
        .nested
        .groupMap(_._1)((v, t) => (v, t._2.sum))
        .orderBy(_._1.asc)
    ).run.map: r =>
      expect.same(Seq((2, Some(5L)), (3, Some(3L)), (5, Some(4L))), r)

  dbTest("GroupByHaving"):
    Select(
      Query
        .values((integer.forgetNNA, integer.forgetNNA))((5, 3), (3, 3), (5, 1), (2, 5))
        .nested
        .groupMap(_._1)((v, t) => (v, t._2.sum))
        .having(_._2.map(_ > 3L.as(bigint)).getOrElse(DbValue.falseV))
        .orderBy(_._1.asc)
    ).run.map: r =>
      expect.same(Set((2, Some(5L)), (5, Some(4L))), r.toSet)

  dbTest("Distinct"):
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).distinct).run.map: r =>
      expect.same(r.toSet, Set(5, 3, 2))

  dbTest("Limit"):
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).limit(3)).run.map: r =>
      expect.same(Set(5, 3, 5), r.toSet)

  dbLogTest("Offset"): log =>
    for
      r <- Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).offset(1)).run
      _ <- log.debug(Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).offset(1)).sqlAndTypes._1.str)
    yield
      expect.same(Set(3, 5, 2), r.toSet)

  dbTest("LimitOffset"):
    Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).limit(3).offset(1)).run.map: r =>
      expect.same(Set(3, 5), r.toSet)

  dbLogTest("OffsetLimit"): log =>
    for
      r <- Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).offset(2).limit(1)).run
      _ <- log.debug(Select(Query.values(integer.forgetNNA)(5, 3, 5, 2).offset(2).limit(1)).sqlAndTypes._1.str)
    yield
      expect.same(Set(5), r.toSet)

  dbTest("Union"):
    Select(Query.values(integer.forgetNNA)(5, 3).union(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      expect.same(Set(5, 3, 2), r.toSet)

  dbTest("UnionAll"):
    Select(Query.values(integer.forgetNNA)(5, 3).unionAll(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
      expect.same(Set(5, 3, 5, 2), r.toSet)

  def doTestIntersect()(using platform.IntersectCapability): Unit =
    dbTest("Intersect"):
      Select(Query.values(integer.forgetNNA)(5, 3).intersect(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
        expect.same(Seq(5), r)

  def doTestIntersectAll()(using platform.IntersectAllCapability): Unit =
    dbTest("IntersectAll"):
      Select(Query.values(integer.forgetNNA)(5, 5, 3).intersectAll(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
        expect.same(Seq(5), r)

  def doTestExcept()(using platform.ExceptCapability): Unit =
    dbTest("Except"):
      Select(Query.values(integer.forgetNNA)(5, 4, 5, 3).except(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
        expect.same(Set(4, 3), r.toSet)

  def doTestExceptAll()(using platform.ExceptAllCapability): Unit =
    dbTest("ExceptAll"):
      Select(Query.values(integer.forgetNNA)(5, 4, 5, 3).exceptAll(Query.values(integer.forgetNNA)(5, 2))).run.map: r =>
        expect.same(Set(4, 5, 3), r.toSet)

  dbTest("Size"):
    Select(Query.of(Query.values(integer.forgetNNA)(5, 3, 5, 2).size)).runOne.map: r =>
      expect.same(4L, r)

  def doTestFlatmapLateral()(using platform.LateralJoinCapability): Unit = dbTest("FlatMap/LateralJoin"):
    Select(
      Query.values(integer.forgetNNA)(5, 3).flatMap(v => Query.values(integer.forgetNNA)(5, 3).map(_ => v))
    ).run.map: r =>
      expect.same(Set(5, 5, 3, 3), r.toSet)
}
