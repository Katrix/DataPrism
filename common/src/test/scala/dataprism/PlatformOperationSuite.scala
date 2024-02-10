package dataprism

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.*
import munit.FunSuite
import perspective.{DistributiveKC, Id}

//noinspection SqlNoDataSourceInspection
trait PlatformOperationSuite[F[_]: MonadThrow, Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[F, Codec0, Platform] {
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

  def makeTestTable(name: String)(using db: DbType): F[Table[Codec0, TestTable]] = for
    _ <- db.run(
      sql"""|CREATE TABLE ${SqlStr.const(name)} (
            |  a INTEGER NOT NULL PRIMARY KEY,
            |  b VARCHAR(254) NOT NULL,
            |  c DOUBLE PRECISION,
            |  d REAL
            |);""".stripMargin
    )
    _ <- db.run(
      sql"""|INSERT INTO ${SqlStr.const(name)} (a, b, c, d) 
            |    VALUES 
            |      (1, 'foo', 3, NULL), 
            |      (2, 'bar', 5.19, 2.1), 
            |      (3, 'baz', NULL, 56), 
            |      (4, 'quox', NULL, NULL);""".stripMargin
    )
  yield testTableObj(name)

  def testWithTable(name: String)(body: DbType ?=> Table[Codec0, TestTable] => F[Seq[TestTable[Id]]]): Unit =
    test(name):
      given DbType = dbFixture()
      for
        table    <- makeTestTable(name)
        expected <- body(table)
        found    <- Select(Query.from(table)).run
      yield assertEquals(found, expected)

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

  def doTestDeleteReturning()(using platform.DeleteReturningCapability): Unit = testWithTable("DeleteReturning"):
    table =>
      Delete
        .from(table)
        .where(_.a === 1.as(integer))
        .returning((v, _) => v.b)
        .run
        .map { returned =>
          assertEquals(returned, Seq("foo"))
          originalTestTableValues.filter(v => v.a != 1 && v.a != 2)
        }

  def doTestDeleteUsingReturning()(using platform.DeleteUsingCapability, platform.DeleteReturningCapability): Unit =
    testWithTable("DeleteUsingReturning"): table =>
      Delete
        .from(table)
        .using(Query.values(integer.forgetNNA)(1, 2))
        .where(_.a === _)
        .returning((v, _) => v.b)
        .run
        .map { returned =>
          assertEquals(returned, Seq("foo", "bar"))
          originalTestTableValues.filter(v => v.a != 1 && v.a != 2)
        }

  testWithTable("InsertValues"): table =>
    val v = TestTable[Id](5, "next", None, None)
    Insert.into(table).values(v).run.map(_ => originalTestTableValues :+ v)

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
      .map(_ => originalTestTableValues.map(v => if v.a == 1 then TestTable(5, "next", None, None) else v))

  def doTestUpdateFrom()(using platform.UpdateFromCapability): Unit = testWithTable("UpdateFrom"): table =>
    Update
      .table(table)
      .from(Query.values(integer.forgetNNA)(1, 2))
      .where(_.a === _)
      .values((v, _) => v.copy(b = "next".as(varchar(256))))
      .run
      .map(_ => originalTestTableValues.map(v => if v.a == 1 || v.a == 2 then v.copy(b = "next") else v))

  def doTestUpdateReturning()(using platform.UpdateReturningCapability): Unit = testWithTable("UpdateReturning"):
    table =>
      Update
        .table(table)
        .where(_.a === 1.as(integer))
        .values(v => v.copy(b = "next".as(varchar(256))))
        .returning((a, _) => a)
        .run
        .map(res => originalTestTableValues.map(v => if v.a == 1 then res.head else v))

  def doTestUpdateFromReturning()(using platform.UpdateFromCapability, platform.UpdateReturningCapability): Unit =
    testWithTable("UpdateFromReturning"): table =>
      Update
        .table(table)
        .from(Query.values(integer.forgetNNA)(1, 2))
        .where(_.a === _)
        .values((v, _) => v.copy(b = "next".as(varchar(256))))
        .returning((_, b) => b)
        .run
        .map { res =>
          assertEquals(res, Seq(1, 2))
          originalTestTableValues.map(v => if v.a == 1 || v.a == 2 then v.copy(b = "next") else v)
        }
}
object PlatformOperationSuite:
  case class TestTable[F[_]](a: F[Int], b: F[String], c: F[Option[Double]], d: F[Option[Float]])
  object TestTable:
    given KMacros.ApplyTraverseKC[TestTable] = KMacros.deriveApplyTraverseKC[TestTable]
    given DistributiveKC[TestTable]          = KMacros.deriveDistributiveKC[TestTable]
