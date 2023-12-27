package dataprism.jdbc

import dataprism.KMacros
import dataprism.jdbc.platform.implementations.PostgresJdbcPlatform
import dataprism.jdbc.sql.{JdbcType, PostgresJdbcTypes}

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.concurrent.Future
import dataprism.platform.base.MapRes
import perspective.*
import perspective.derivation.{ProductK, ProductKPar}
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

  val table: Table[HomeK, JdbcType] = Table(
    "homes",
    HomeK(
      owner = Column("owner", PostgresJdbcTypes.uuid),
      name = Column("name", PostgresJdbcTypes.text),
      createdAt =
        Column("created_at", PostgresJdbcTypes.timestampWithTimezone.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC))),
      updatedAt =
        Column("updated_at", PostgresJdbcTypes.timestampWithTimezone.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC))),
      x = Column("x", PostgresJdbcTypes.doublePrecision),
      y = Column("y", PostgresJdbcTypes.doublePrecision),
      z = Column("z", PostgresJdbcTypes.doublePrecision),
      yaw = Column("yaw", PostgresJdbcTypes.real),
      pitch = Column("pitch", PostgresJdbcTypes.real),
      worldUuid = Column("world_uuid", PostgresJdbcTypes.uuid)
    )
  )

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

  val table: Table[ResidentK, JdbcType] = Table(
    "home_residents",
    ResidentK(
      Column("home_owner", PostgresJdbcTypes.uuid),
      Column("home_name", PostgresJdbcTypes.text),
      Column("resident", PostgresJdbcTypes.uuid),
      Column("created_at", PostgresJdbcTypes.timestampWithTimezone.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC)))
    )
  )

  given KMacros.RepresentableTraverseKC[ResidentK] = KMacros.deriveRepresentableTraverseKC[ResidentK]
}

case class DepartmentK[F[_]](dpt: F[String])
object DepartmentK {
  given KMacros.RepresentableTraverseKC[DepartmentK] = KMacros.deriveRepresentableTraverseKC[DepartmentK]

  val table: Table[DepartmentK, JdbcType] = Table("departments", DepartmentK(Column("dpt", PostgresJdbcTypes.text)))
}

case class EmployeK[F[_]](dpt: F[String], emp: F[String])
object EmployeK {
  given KMacros.RepresentableTraverseKC[EmployeK] = KMacros.deriveRepresentableTraverseKC[EmployeK]

  val table: Table[EmployeK, JdbcType] =
    Table("employees", EmployeK(Column("dpt", PostgresJdbcTypes.text), Column("emp", PostgresJdbcTypes.text)))
}

case class TaskK[F[_]](emp: F[String], tsk: F[String])
object TaskK {
  given KMacros.RepresentableTraverseKC[TaskK] = KMacros.deriveRepresentableTraverseKC[TaskK]

  val table: Table[TaskK, JdbcType] =
    Table("tasks", TaskK(Column("emp", PostgresJdbcTypes.text), Column("tsk", PostgresJdbcTypes.text)))
}

case class FooK[F[_]](a: F[Int], b: F[Int], c: FooK.CK[F])
object FooK {
  given KMacros.RepresentableTraverseKC[FooK] = KMacros.deriveRepresentableTraverseKC[FooK]

  case class CK[F[_]](d: F[Double], e: F[Double])
  object CK {
    given KMacros.RepresentableTraverseKC[CK] = KMacros.deriveRepresentableTraverseKC[CK]
  }

}

object Testing {

  import PostgresJdbcPlatform.*

  def printSqlStr(str: SqlStr[JdbcType]): Unit =
    if str.str.isEmpty then println("ERROR: Empty string") else println(str.str)
    println()

  def printQuery[A[_[_]]](q: Query[A]): Unit =
    printSqlStr(PostgresJdbcPlatform.sqlRenderer.renderSelect(q.selectAst))

  given Db[Future, JdbcType] with {
    override def run(sql: SqlStr[JdbcType]): Future[Int] =
      printSqlStr(sql)
      Future.successful(0)

    override def runIntoSimple[Res](sql: SqlStr[JdbcType], dbTypes: JdbcType[Res]): Future[QueryResult[Res]] =
      printSqlStr(sql)
      Future.successful(QueryResult(Nil))

    override def runIntoRes[Res[_[_]]](sql: SqlStr[JdbcType], dbTypes: Res[JdbcType])(
        using FA: ApplyKC[Res],
        FT: TraverseKC[Res]
    ): Future[QueryResult[Res[Id]]] =
      printSqlStr(sql)
      Future.successful(QueryResult(Nil))
  }

  case class Location(getX: Double, getY: Double, getZ: Double)

  @main def testingDef: Unit =
    val uuid       = UUID.randomUUID()
    val homeOwner  = UUID.randomUUID()
    val name       = "name"
    val homeName   = name
    val worldUuid  = UUID.randomUUID()
    val residentId = UUID.randomUUID()

    val home     = HomeK[Id](homeOwner, homeName, Instant.now(), Instant.now(), 5, 10, 15, 20, 25, worldUuid)
    val resident = ResidentK[Id](homeOwner, homeName, residentId, Instant.now())

    /*
    Select(Query.from(HomeK.table).filter(_.owner === uuid.as(DbType.uuid))).run

    Select(
      Query
        .from(HomeK.table)
        .filter(home => home.owner === uuid.as(DbType.uuid) && home.name === name.as(DbType.text))
    ).run

    Select(Query.of(Query.from(HomeK.table).filter(_.owner === uuid.as(DbType.uuid)).size)).run

    Select(
      Query.of(
        Query
          .from(HomeK.table)
          .filter(home => home.owner === uuid.as(DbType.uuid) && home.name === name.as(DbType.text))
          .nonEmpty
      )
    ).run

    Insert.into(HomeK.table).values(Query.valuesOf(HomeK.table, home)).onConflictUpdate.run

    Delete
      .from(HomeK.table)
      .where((home, _) => home.owner === uuid.as(DbType.uuid) && home.name === name.as(DbType.text))
      .run

    def searchHomes(
        location: Location,
        radius: Option[Double],
        world: Option[UUID],
        owner: Option[UUID],
        drop: Int,
        take: Int
    ): Unit =
      def square2(v: DbValue[Double]): DbValue[Double] = v * v

      val distanceToPlayerSq2: HomeK[DbValue] => DbValue[Double] = h =>
        square2(h.x - location.getX.as(DbType.double)) +
          square2(h.y - location.getY.as(DbType.double)) +
          square2(h.z - location.getZ.as(DbType.double))

      val filters2: Seq[Option[Query[HomeK] => Query[HomeK]]] = Seq(
        radius.map { r =>
          _.filter { h =>
            distanceToPlayerSq2(h) < (r * r).as(DbType.double)
          }
        },
        world.map(w => _.filter(_.worldUuid === w.as(DbType.uuid))),
        owner.map(o => _.filter(_.owner === o.as(DbType.uuid)))
      )

      Select(
        filters2.flatten
          .foldLeft(Query.from(HomeK.table))((q, f) => f(q))
          .orderBy(h => distanceToPlayerSq2(h).asc)
          .drop(drop)
          .take(take)
      ).run
    end searchHomes

    searchHomes(Location(0, 0, 0), None, None, None, 0, 0)
    searchHomes(Location(0, 0, 0), Some(5), None, None, 0, 0)
    searchHomes(Location(0, 0, 0), None, Some(uuid), None, 0, 0)
    searchHomes(Location(0, 0, 0), None, None, Some(uuid), 0, 0)
    searchHomes(Location(0, 0, 0), None, Some(uuid), Some(uuid), 0, 0)
    searchHomes(Location(0, 0, 0), Some(5), Some(uuid), None, 0, 0)
    searchHomes(Location(0, 0, 0), Some(5), Some(uuid), Some(uuid), 0, 0)

    Select(
      Query
        .from(ResidentK.table)
        .filter(resident => resident.owner === homeOwner.as(DbType.uuid))
        .groupMap(_.homeName)((name, r) => (name, r.resident.arrayAgg))
    ).run

    Select(
      Query
        .from(ResidentK.table)
        .filter(r => r.owner === homeOwner.as(DbType.uuid) && r.homeName === homeName.as(DbType.text))
        .map(_.resident)
    ).run

    Select(
      Query.of(
        Query
          .from(ResidentK.table)
          .filter(r => r.owner === homeOwner.as(DbType.uuid) && r.homeName === homeName.as(DbType.text))
          .nonEmpty
      )
    ).run

    Insert.into(ResidentK.table).values(Query.valuesOf(ResidentK.table, resident)).run

    Delete
      .from(ResidentK.table)
      .where((r, _) => r.owner === homeOwner.as(DbType.uuid) && r.homeName === homeName.as(DbType.text))
      .run

    Select(
      Query.from(HomeK.table).map(_.owner).distinct
    ).run

    Select(Query.from(HomeK.table)).run
    Select(Query.from(ResidentK.table)).run

    Delete.from(HomeK.table).where((_, _) => true.as(DbType.boolean)).run
    Delete.from(ResidentK.table).where((_, _) => true.as(DbType.boolean)).run

    Insert.into(HomeK.table).values(Query.valuesOf(HomeK.table, home, Seq(home))).run
    Insert.into(ResidentK.table).values(Query.valuesOf(ResidentK.table, resident, Seq(resident))).run
     */

    /*
    def noneHome[F[_]]: HomeK[Compose2[Option, F]] = HomeK(None, None, None, None, None, None, None, None, None, None)

    Update
      .table(HomeK.table)
      .where((h, _) => h.owner === homeOwner.as(DbType.uuid))
      .someValues((h, _) => noneHome.copy(owner = Some(homeOwner.as(DbType.uuid)), x = Some(h.x + h.x)))
      .run
     */

    Insert
      .into(ResidentK.table)
      .valuesWithoutSomeColumns(
        Query.valueOfOpt(
          ResidentK.table,
          ResidentK(
            Some(UUID.randomUUID()),
            Some("foo"),
            Some(UUID.randomUUID()),
            None
          )
        )
      )

    val mr1 = summon[MapRes[DbValue, (DbValue[String], DbValue[UUID])]]
    val mr2 = summon[MapRes[DbValue, mr1.K[DbValue]]]

    printQuery(
      Query
        .from(ResidentK.table)
        .filter(resident => resident.owner === homeOwner.as(PostgresJdbcTypes.uuid))
        .map(r => (r.homeName, r.resident))
      // .groupMap(_.homeName)((name, r) => (name, r.resident.arrayAgg))
    )

    ()
}
