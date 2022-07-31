# DataPrism

DataPrism as an early in dev FRM (Functional Relational Mapper).

DataPrism focuses on doing its job by using HKD(Higher Kinded Data).

Simple showcase of code
```scala
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


Query.from(HomeK.table).mapT(homes => (homes.owner, homes.name))

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
```