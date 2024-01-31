package dataprism.skunk

import java.time.{Instant, ZoneOffset}
import java.util.UUID

import cats.effect.IO
import dataprism.KMacros
import dataprism.skunk.platform.PostgresSkunkPlatform
import dataprism.skunk.sql.SkunkAnsiTypes.*
import dataprism.skunk.sql.SkunkDb
import dataprism.sql.*
import perspective.*
import skunk.codec.all.*
import skunk.{Codec, Session}

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

  val table: Table[Codec, HomeK] = Table(
    "homes",
    HomeK(
      owner = Column("owner", uuid.wrap),
      name = Column("name", text.wrap),
      createdAt = Column("created_at", timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC)).wrap),
      updatedAt = Column("updated_at", timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC)).wrap),
      x = Column("x", float8.wrap),
      y = Column("y", float8.wrap),
      z = Column("z", float8.wrap),
      yaw = Column("yaw", float4.wrap),
      pitch = Column("pitch", float4.wrap),
      worldUuid = Column("world_uuid", uuid.wrap)
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

  val table: Table[Codec, ResidentK] = Table(
    "home_residents",
    ResidentK(
      Column("home_owner", uuid.wrap),
      Column("home_name", text.wrap),
      Column("resident", uuid.wrap),
      Column("created_at", timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC)).wrap)
    )
  )

  given KMacros.RepresentableTraverseKC[ResidentK] = KMacros.deriveRepresentableTraverseKC[ResidentK]
}

object Testing {
  import PostgresSkunkPlatform.Api.*

  val session: Session[IO] = ???
  given db: SkunkDb[IO]    = SkunkDb(session)

  Select(Query.from(HomeK.table).filter(_.name === "some_name".as(text.wrap))).run

  val homeQuery: skunk.Query[(UUID, String), HomeK[Id]] =
    Compile.query((uuid.wrap.notNull.forgetNNA, text.wrap.notNull.forgetNNA)) { case (ownerId, homeName) =>
      Select(Query.from(HomeK.table).filter(h => h.name === homeName && h.owner === ownerId))
    }
}
