package dataprism.skunk

import perspective.*
import cats.effect.IO
import dataprism.KMacros
import dataprism.skunk.platform.implementations.PostgresSkunkPlatform
import dataprism.skunk.sql.SkunkDb
import dataprism.sql.*
import skunk.{Codec, Session}
import skunk.codec.all.*

import java.time.{Instant, ZoneOffset}
import java.util.UUID

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

  val table: Table[HomeK, Codec] = Table(
    "homes",
    HomeK(
      owner = Column("owner", uuid),
      name = Column("name", text),
      createdAt =
        Column("created_at", timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC))),
      updatedAt =
        Column("updated_at", timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC))),
      x = Column("x", float8),
      y = Column("y", float8),
      z = Column("z", float8),
      yaw = Column("yaw", float4),
      pitch = Column("pitch", float4),
      worldUuid = Column("world_uuid", uuid)
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

  val table: Table[ResidentK, Codec] = Table(
    "home_residents",
    ResidentK(
      Column("home_owner", uuid),
      Column("home_name", text),
      Column("resident", uuid),
      Column("created_at", timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC)))
    )
  )
  
  given KMacros.RepresentableTraverseKC[ResidentK] = KMacros.deriveRepresentableTraverseKC[ResidentK]
}

object Testing {
  import PostgresSkunkPlatform.*

  val session: Session[IO] = ???
  given db: SkunkDb[IO] = SkunkDb(session)

  Select(Query.from(HomeK.table).filter(_.name === "some_name".as(text))).run

  val homeQuery: skunk.Query[(UUID, String), HomeK[Id]] = Compile.query((uuid, text)) { case (ownerId, homeName) =>
    Select(Query.from(HomeK.table).filter(h => h.name === homeName && h.owner === ownerId))
  }
}
