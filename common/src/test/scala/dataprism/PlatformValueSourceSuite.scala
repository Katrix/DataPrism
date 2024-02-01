package dataprism

import cats.MonadThrow
import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.*
import munit.FunSuite
import perspective.Id

trait PlatformValueSourceSuite[F[_]: MonadThrow, Codec0[_]] extends PlatformFunSuite[F, Codec0] {
  import platform.AnsiTypes
  import platform.Api.*

  private val testQuery = Query.values(AnsiTypes.integer.forgetNNA)(5, 3)

  test("FromQuery"):
    given DbType = dbFixture()
    Select(testQuery.nested)
      .run[F]
      .map: r =>
        assertEquals(r, Seq(5, 3))

  case class TempTable[G[_]](i: G[Option[Int]], d: G[Option[Double]])
  object TempTable:
    given KMacros.ApplyTraverseKC[TempTable] = KMacros.deriveApplyTraverseKC[TempTable]

    val table: Table[Codec0, TempTable] = Table(
      "tempTable",
      TempTable(
        Column("i", AnsiTypes.integer.nullable),
        Column("d", AnsiTypes.doublePrecision.nullable)
      )
    )

  test("FromTable"):
    given db: DbType = dbFixture()
    for
      _ <- db.run(sql"CREATE TABLE tempTable (i INTEGER, d DOUBLE PRECISION);")
      _ <- db.run(sql"INSERT INTO tempTable (i, d) VALUES (5, 3.14)")
      r <- Select(Query.from(TempTable.table)).run
    yield assertEquals(r, Seq(TempTable[Id](Some(5), Some(3.14D))))

  test("InnerJoin"):
    given DbType = dbFixture()
    Select(testQuery.join(testQuery)(_ === _))
      .run[F]
      .map: r =>
        assertEquals(r, Seq((5, 5), (3, 3)))

  test("CrossJoin"):
    given DbType = dbFixture()
    Select(testQuery.crossJoin(testQuery))
      .run[F]
      .map: r =>
        assertEquals(r, Seq((5, 5), (5, 3), (3, 3), (3, 5)))

  test("LeftJoin"):
    given DbType = dbFixture()
    Select(testQuery.leftJoin(testQuery)(_ === _))
      .run[F]
      .map: r =>
        assertEquals(r, Seq((5, Some(5)), (3, Some(3))))

  test("RightJoin"):
    given DbType = dbFixture()
    Select(testQuery.rightJoin(testQuery)(_ === _))
      .run[F]
      .map: r =>
        assertEquals(r, Seq((Some(5), 5), (Some(3), 3)))

  test("FullJoin"):
    given DbType = dbFixture()
    Select(testQuery.fullJoin(testQuery)(_ === _))
      .run[F]
      .map: r =>
        assertEquals(r, Seq((Some(5), Some(5)), (Some(3), Some(3))))

}
