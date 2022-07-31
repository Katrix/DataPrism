package dataprism

import java.time.Instant
import java.util.UUID

import dataprism.platform.PostgresQueryPlatform
import dataprism.sql.*

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

object Testing {

  @main def testingDef: Unit =
    val platform = new PostgresQueryPlatform
    import platform.*

    def printQuery[A[_[_]]](q: Query[A]): Unit =
      println(q.selectRender.str)
      println()

    printQuery(
      Query
        .from(HomeK.table)
        .groupByT(homes =>
          (
            homes.owner.groupedBy,
            homes.name.groupedBy,
            homes.x.asMany.arrayAgg,
            homes.y.asMany.arrayAgg,
            homes.z.asMany.arrayAgg
          )
        )
    )
}
