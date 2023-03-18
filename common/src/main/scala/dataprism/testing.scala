package dataprism

import java.time.Instant
import java.util.UUID

import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.sql.*
import perspective.derivation.{ProductK, ProductKPar}

case class HomeK[F[_]](
    owner: F[UUID],
    name: F[String],
    createdAt: F[Instant],
    updatedAt: F[Instant],
    x: F[Double],
    y: F[Double],
    z: F[Double],
    yaw: F[Float],
    pitch: F[Float],
    worldUuid: F[UUID]
)

object HomeK {
  given instance[F[_]](
      using uuid: F[UUID],
      string: F[String],
      instant: F[Instant],
      double: F[Double],
      float: F[Float]
  ): HomeK[F] =
    HomeK(uuid, string, instant, instant, double, double, double, float, float, uuid)

  val table: Table[HomeK] = Table(
    "homes",
    HomeK(
      owner = Column("owner", DbType.uuid),
      name = Column("name", DbType.text),
      createdAt = Column("created_at", DbType.timestamptz),
      updatedAt = Column("updated_at", DbType.timestamptz),
      x = Column("x", DbType.double),
      y = Column("y", DbType.double),
      z = Column("z", DbType.double),
      yaw = Column("yaw", DbType.float),
      pitch = Column("pitch", DbType.float),
      worldUuid = Column("world_uuid", DbType.uuid)
    )
  )

  given HomeK[DbType] = Table.tableDbTypes(table)

  given typeclass: KMacros.RepresentableTraverseKC[HomeK] = KMacros.deriveRepresentableTraverseKC[HomeK]
}

case class ResidentK[F[_]](
    owner: F[UUID],
    homeName: F[String],
    resident: F[UUID],
    createdAt: F[Instant]
)

object ResidentK {
  given instance[F[_]](
      using uuid: F[UUID],
      string: F[String],
      instant: F[Instant]
  ): ResidentK[F] =
    ResidentK(uuid, string, uuid, instant)

  val table: Table[ResidentK] = Table(
    "home_residents",
    ResidentK(
      Column("home_owner", DbType.uuid),
      Column("home_name", DbType.text),
      Column("resident", DbType.uuid),
      Column("created_at", DbType.timestamptz)
    )
  )

  given ResidentK[DbType] = Table.tableDbTypes(table)

  given KMacros.RepresentableTraverseKC[ResidentK] = KMacros.deriveRepresentableTraverseKC[ResidentK]
}

case class People[F[_]](name: F[String], age: F[Option[Int]])
object People {

  val table: Table[People] = Table(
    "people",
    People(
      Column("name", DbType.text),
      Column("age", DbType.nullable(DbType.int32))
    )
  )

  given People[DbType] = Table.tableDbTypes(table)

  given KMacros.RepresentableTraverseKC[People] = KMacros.deriveRepresentableTraverseKC[People]
}

case class Names[F[_]](name: F[String])
object Names {
  given Names[DbType] = Names(DbType.text)

  given KMacros.RepresentableTraverseKC[Names] = KMacros.deriveRepresentableTraverseKC[Names]
}

object Testing {

  @main def testingDef: Unit =
    val platform = new PostgresQueryPlatform
    import platform.*

    def printQuery[A[_[_]]](q: Query[A]): Unit =
      println(platform.sqlRenderer.renderSelect(q.selectAst).str)
      println()

    /*
    printQuery(Query.from(HomeK.table).mapT(homes => (homes.owner, homes.name)))
    printQuery(
      Query
        .from(HomeK.table)
        .mapT(homes => (homes.owner, homes.name))
        .filter(t => t.tuple.head === t.tuple.head && t.tuple.tail.head === t.tuple.tail.head)
    )
    printQuery(
      Query
        .from(HomeK.table)
        .mapT(homes => (homes.owner, homes.name))
        .limit(5)
        .mapT(t => (t.tuple.head, t.tuple.head))
    )

    printQuery(
      Query.from(HomeK.table).join(ResidentK.table)((h, r) => h.owner === r.owner && h.name === r.homeName)
    )

    printQuery(
      Query.from(HomeK.table).map(home => home)
    )
    */

    printQuery(
      Query
        .from(HomeK.table)
        .groupMap(homes => (homes.owner, homes.name))((grouped, homes) =>
          (
            grouped._1,
            grouped._2,
            homes.x.arrayAgg,
            homes.y.arrayAgg,
            homes.z.arrayAgg
          )
        ).having(t => t.tuple._2 === "foo".as(DbType.text))
    )


    printQuery(
      for
        home <- Query.from(HomeK.table)
      yield home.owner
    )
}
