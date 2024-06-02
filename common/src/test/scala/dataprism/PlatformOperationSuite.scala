package dataprism

import cats.data.NonEmptyList
import cats.kernel.Eq
import cats.syntax.all.*
import cats.{MonadThrow, Monoid}
import dataprism.platform.MapRes
import dataprism.platform.sql.{SqlMergeOperations, SqlQueryPlatform}
import dataprism.sql.*
import perspective.{DistributiveKC, Id}
import weaver.{Expectations, SourceLocation, TestName}

//noinspection SqlNoDataSourceInspection
trait PlatformOperationSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[Codec0, Platform] {
  import PlatformOperationSuite.TestTable
  import platform.AnsiTypes.*
  import platform.Api.*

  // No need to test Selects, it's been tested all throughout the test suite

  def testTableObj(name: String): Table[Codec0, TestTable] = Table(
    name,
    TestTable(
      Column("a", integer),
      Column("b", varchar(254)),
      Column("c", doublePrecision.nullable),
      Column("d", real.nullable)
    )
  )

  val originalTestTableValues: Seq[TestTable[Id]] = Seq(
    TestTable(1, "foo", Some(3), None),
    TestTable(2, "bar", Some(5.19), Some(2.1F)),
    TestTable(3, "baz", None, Some(56F)),
    TestTable(4, "quox", None, None)
  )

  def makeTestTable(name: String)(using db: DbType): F[Table[Codec0, TestTable]] =
    def quoteConst(s: String) = SqlStr.const(platform.sqlRenderer.quote(s))
    for
      _ <- db.run(
        sql"""|CREATE TABLE ${quoteConst(name)} (
              |  ${quoteConst("a")} INTEGER NOT NULL PRIMARY KEY,
              |  ${quoteConst("b")} VARCHAR(254) NOT NULL,
              |  ${quoteConst("c")} DOUBLE PRECISION,
              |  ${quoteConst("d")} REAL
              |);""".stripMargin
      )
      _ <- db.run(
        sql"""|INSERT INTO ${quoteConst(name)} (${quoteConst("a")}, ${quoteConst("b")}, ${quoteConst(
               "c"
             )}, ${quoteConst("d")})
              |    VALUES 
              |      (1, 'foo', 3, NULL), 
              |      (2, 'bar', 5.19, 2.1), 
              |      (3, 'baz', NULL, 56), 
              |      (4, 'quox', NULL, NULL);""".stripMargin
      )
    yield testTableObj(name)

  def testWithTableAndExpectation(name: TestName)(
      body: DbType ?=> Table[Codec0, TestTable] => F[(Expectations, Seq[TestTable[Id]])]
  )(
      using loc: SourceLocation
  ): Unit =
    dbTest(name):
      for
        table <- makeTestTable(name.name)
        t     <- body(table)
        (expectations, expected) = t
        found <- Select(Query.from(table)).run
      yield expectations && expect.same(expected.toSet, found.toSet)(using Eq.fromUniversalEquals, loc = loc)

  def testWithTable(name: TestName)(body: DbType ?=> Table[Codec0, TestTable] => F[Seq[TestTable[Id]]])(
      using SourceLocation
  ): Unit = testWithTableAndExpectation(name)(table => body(table).map(r => (Monoid[Expectations].empty, r)))

  testWithTable("Delete"): table =>
    Delete
      .from(table)
      .where(_.a === 1.as(integer))
      .run
      .map(_ => originalTestTableValues.filter(_.a != 1))

  def doTestDeleteUsing()(using platform.DeleteUsingCapability): Unit = testWithTable("DeleteUsing"): table =>
    Delete
      .from(table)
      .using(Query.values(integer.forgetNNA)(1, 2))
      .where(_.a === _)
      .run
      .map(_ => originalTestTableValues.filter(v => v.a != 1 && v.a != 2))

  def doTestDeleteReturning()(using platform.DeleteReturningCapability): Unit =
    testWithTableAndExpectation("DeleteReturning"): table =>
      Delete
        .from(table)
        .where(_.a === 1.as(integer))
        .returning((v, _) => v.b)
        .run
        .map { returned =>
          (expect.same(Seq("foo"), returned), originalTestTableValues.filter(v => v.a != 1))
        }

  def doTestDeleteUsingReturning()(using platform.DeleteUsingCapability, platform.DeleteReturningCapability): Unit =
    testWithTableAndExpectation("DeleteUsingReturning"): table =>
      Delete
        .from(table)
        .using(Query.values(integer.forgetNNA)(1, 2))
        .where(_.a === _)
        .returning((v, _) => v.b)
        .run
        .map { returned =>
          (expect.same(Seq("foo", "bar"), returned), originalTestTableValues.filter(v => v.a != 1 && v.a != 2))
        }

  testWithTable("InsertValues"): table =>
    val v  = TestTable[Id](5, "next", None, None)
    val vs = Seq(TestTable[Id](6, "next2", None, None))
    Insert.into(table).values(v, vs*).run.map(_ => originalTestTableValues ++ (v +: vs))

  testWithTable("InsertValuesBatch"): table =>
    val v  = TestTable[Id](5, "next", None, None)
    val vs = Seq(TestTable[Id](6, "next2", None, None))
    Insert.into(table).valuesBatch(v, vs*).run.map(_ => originalTestTableValues ++ (v +: vs))

  testWithTable("InsertSomeValues"): table =>
    Insert
      .into(table)
      .valuesInColumns(t => (t.a, t.b))((5, "next"))
      .run
      .map(_ => originalTestTableValues :+ TestTable[Id](5, "next", None, None))

  testWithTable("InsertQuery"): table =>
    Insert
      .into(table)
      .valuesFromQuery(
        Query.of(
          TestTable[DbValue](
            5.as(integer),
            "next".as(varchar(254)),
            DbValue.nullV(doublePrecision),
            DbValue.nullV(real)
          )
        )
      )
      .run
      .map(_ => originalTestTableValues :+ TestTable[Id](5, "next", None, None))

  def doTestInsertReturning()(using platform.InsertReturningCapability): Unit = testWithTable("InsertQueryReturning"):
    table =>
      Insert
        .into(table)
        .valuesFromQuery(
          Query.of(
            TestTable[DbValue](
              5.as(integer),
              "next".as(varchar(254)),
              DbValue.nullV(doublePrecision),
              DbValue.nullV(real)
            )
          )
        )
        .returning(identity)
        .run
        .map(res => originalTestTableValues ++ res)

  def doTestInsertOnConflict()(using platform.InsertOnConflictCapability): Unit = testWithTable("InsertOnConflict"):
    table =>
      Insert
        .into(table)
        .values(TestTable[Id](1, "next", None, None))
        .onConflictUpdate(v => NonEmptyList.of(v.a))
        .run
        .map(_ => originalTestTableValues.map(v => if v.a == 1 then TestTable(1, "next", None, None) else v))

  def doTestInsertOnConflictReturning()(
      using platform.InsertOnConflictCapability,
      platform.InsertReturningCapability
  ): Unit = testWithTable("InsertOnConflictReturning"): table =>
    Insert
      .into(table)
      .values(TestTable[Id](1, "next", None, None))
      .onConflictUpdate(v => NonEmptyList.of(v.a))
      .returning(identity)
      .run
      .map(res => originalTestTableValues.map(v => if v.a == 1 then res.head else v))

  testWithTable("UpdateAll"): table =>
    Update
      .table(table)
      .where(_.a === 1.as(integer))
      .values(_ =>
        TestTable(5.as(integer), "next".as(varchar(254)), DbValue.nullV(doublePrecision), DbValue.nullV(real))
      )
      .run
      .map(_ => originalTestTableValues.map(v => if v.a == 1 then TestTable(5, "next", None, None) else v))

  testWithTable("UpdateSome"): table =>
    Update
      .table(table)
      .where(_.a === 1.as(integer))
      .valuesInColumns(_.b)(_ => "next".as(varchar(254)))
      .run
      .map(_ => originalTestTableValues.map(v => if v.a == 1 then v.copy(b = "next") else v))

  def doTestUpdateFrom()(using platform.UpdateFromCapability): Unit = testWithTable("UpdateFrom"): table =>
    Update
      .table(table)
      .from(Query.values(integer.forgetNNA)(1, 2))
      .where(_.a === _)
      .values((v, _) => v.copy(b = "next".as(varchar(256))))
      .run
      .map(_ => originalTestTableValues.map(v => if v.a == 1 || v.a == 2 then v.copy(b = "next") else v))

  def doTestUpdateReturning(
      f: platform.MapUpdateReturning[TestTable[platform.DbValue], TestTable[platform.DbValue], TestTable[
        platform.DbValue
      ]]
  )(using platform.UpdateReturningCapability): Unit =
    testWithTable("UpdateReturning"): table =>
      Update
        .table(table)
        .where(_.a === 1.as(integer))
        .values(v => v.copy(b = "next".as(varchar(256))))
        .returning(f)
        .run
        .map(res => originalTestTableValues.map(v => if v.a == 1 then res.head else v))

  def doTestUpdateFromReturning[Res](
      f: platform.MapUpdateReturning[TestTable[platform.DbValue], platform.DbValue[Int], platform.DbValue[Res]],
      expected: Seq[Res]
  )(using platform.UpdateFromCapability, platform.UpdateReturningCapability): Unit =
    testWithTableAndExpectation("UpdateFromReturning"): table =>
      Update
        .table(table)
        .from(Query.values(integer.forgetNNA)(1, 2))
        .where(_.a === _)
        .values((v, _) => v.copy(b = "next".as(varchar(256))))
        .returning(f)
        .run
        .map { res =>
          (
            expect.same(expected, res),
            originalTestTableValues.map(v => if v.a == 1 || v.a == 2 then v.copy(b = "next") else v)
          )
        }

  def doTestMerge(platform: Platform & SqlMergeOperations): Unit =
    import platform.Api.*

    testWithTable("MergeDelete"): table =>
      Merge
        .into(table)
        .using(Query.values(integer.forgetNNA)(1, 3))
        .on(_.a === _)
        .whenMatched
        .thenDelete
        .run
        .map(_ => originalTestTableValues.filter(t => t.a != 1 && t.a != 3))

    testWithTable("MergeDeleteAnd"): table =>
      Merge
        .into(table)
        .using(Query.values((integer.forgetNNA, defaultStringType.forgetNNA))((1, "foo"), (3, "foo")))
        .on(_.a === _._1)
        .whenMatched
        .and(_.b === _._2)
        .thenDelete
        .run
        .map(_ => originalTestTableValues.filter(t => t.a != 1))

    testWithTable("MergeUpdate"): table =>
      Merge
        .into(table)
        .using(Query.values((integer.forgetNNA, defaultStringType.forgetNNA))((1, "baz"), (3, "foo")))
        .on(_.a === _._1)
        .whenMatched
        .thenUpdate
        .valuesInColumns(_.b)((_, b) => b._2)
        .run
        .map(_ =>
          originalTestTableValues.map(t =>
            if t.a == 1 then t.copy(b = "baz") else if t.a == 3 then t.copy(b = "foo") else t
          )
        )

    testWithTable("MergeUpdateAnd"): table =>
      Merge
        .into(table)
        .using(
          Query.values((integer.forgetNNA, defaultStringType.forgetNNA, defaultStringType.forgetNNA))(
            (1, "foo", "baz"),
            (3, "foo", "baz")
          )
        )
        .on(_.a === _._1)
        .whenMatched
        .and(_.b === _._2)
        .thenUpdate
        .valuesInColumns(_.b)((a, b) => b._3)
        .run
        .map(_ => originalTestTableValues.map(t => if t.a == 1 then t.copy(b = "baz") else t))

    val newV1 = TestTable[Id](5, "newfoo", Some(9.9), Some(-1))
    val newV2 = TestTable[Id](6, "newbar", None, Some(-1.9F))

    testWithTable("MergeInsert"): table =>
      Merge
        .into(table)
        .using(Query.valuesOf(table, newV1, newV2))
        .on(_.a === _._1)
        .whenNotMatched
        .thenInsert
        .values(identity)
        .run
        .map(_ => originalTestTableValues :+ newV1 :+ newV2)

    testWithTable("MergeInsertAnd"): table =>
      Merge
        .into(table)
        .using(Query.valuesOf(table, newV1, newV2))
        .on(_.a === _.a)
        .whenNotMatched
        .and(_.b === "newfoo".as(defaultStringType))
        .thenInsert
        .values(identity)
        .run
        .map(_ => originalTestTableValues :+ newV1)

    testWithTable("MergeUpdateOrInsert"): table =>
      Merge
        .into(table)
        .using(Query.valuesOf(table, newV1, newV2.copy(a = 1)))
        .on(_.a === _.a)
        .whenMatched
        .thenUpdate
        .values((_, b) => b)
        .whenNotMatched
        .thenInsert
        .values(identity)
        .run
        .map(_ => originalTestTableValues.map(t => if t.a == 1 then newV2.copy[Id](a = 1) else t) :+ newV1)
}
object PlatformOperationSuite:
  case class TestTable[F[_]](a: F[Int], b: F[String], c: F[Option[Double]], d: F[Option[Float]])
  object TestTable:
    given KMacros.ApplyTraverseKC[TestTable] = KMacros.deriveApplyTraverseKC[TestTable]
    given DistributiveKC[TestTable]          = KMacros.deriveDistributiveKC[TestTable]
