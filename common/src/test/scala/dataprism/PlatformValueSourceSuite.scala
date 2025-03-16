package dataprism

import cats.syntax.all.*
import dataprism.platform.sql.SqlQueryPlatform
import dataprism.sql.*
import perspective.Id
import weaver.Expectations

trait PlatformValueSourceSuite[Codec0[_], Platform <: SqlQueryPlatform { type Codec[A] = Codec0[A] }]
    extends PlatformFunSuite[Codec0, Platform] {
  import platform.AnsiTypes
  import platform.Api.*

  private val testQuery = Query.values(AnsiTypes.integer.forgetNNA)(5, 3)

  dbTest("FromQuery"):
    Select(testQuery.nested)
      .run[F]
      .map: r =>
        expect.same(Seq(5, 3), r)

  case class TempTable[G[_]](i: G[Int | SqlNull], d: G[Double | SqlNull])
  object TempTable:
    given KMacros.ApplyTraverseKC[TempTable] = KMacros.deriveApplyTraverseKC[TempTable]

    val table: Table[Codec0, TempTable] = Table(
      "tempTable",
      TempTable(
        Column("i", AnsiTypes.integer.nullable),
        Column("d", AnsiTypes.doublePrecision.nullable)
      )
    )

  dbTest("FromTable"): db ?=>
    def q(s: String) = SqlStr.const(platform.sqlRenderer.quote(s))
    for
      res <- db.run(
        sql"""CREATE TABLE ${q("tempTable")} (${q("i")} INTEGER, ${q("d")} DOUBLE PRECISION);"""
      )
      _ <- db.run(
        sql"""INSERT INTO ${q("tempTable")} (${q("i")}, ${q("d")}) VALUES (5, 3.14)"""
      )
      r <- Select(Query.from(TempTable.table)).run
    yield expect.same(Seq(TempTable[Id](5, 3.14D)), r)

  dbTest("InnerJoin"):
    Select(testQuery.join(testQuery)(_ === _))
      .run[F]
      .map: r =>
        expect.same(Seq((5, 5), (3, 3)), r)

  dbTest("CrossJoin"):
    Select(testQuery.crossJoin(testQuery))
      .run[F]
      .map: r =>
        expect.same(Set((5, 5), (5, 3), (3, 3), (3, 5)), r.toSet)

  dbTest("LeftJoin"):
    Select(testQuery.leftJoin(testQuery)(_ === _))
      .run[F]
      .map: r =>
        expect.same(Seq((5, 5), (3, 3)), r)

  dbTest("RightJoin"):
    Select(testQuery.rightJoin(testQuery)(_ === _))
      .run[F]
      .map: r =>
        expect.same(Seq((5, 5), (3, 3)), r)

  def doTestFullJoin()(using platform.FullJoinCapability): Unit =
    dbTest("FullJoin"):
      Select(testQuery.fullJoin(testQuery)(_ === _))
        .run[F]
        .map: r =>
          expect.same(Seq((5, 5), (3, 3)), r)

}
