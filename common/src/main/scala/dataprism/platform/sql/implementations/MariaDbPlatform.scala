package dataprism.platform.sql.implementations

import dataprism.sharedast.MariaDbAstRenderer

trait MariaDbPlatform extends MySqlPlatform {
  given IntersectAllCapability with {}
  given ExceptAllCapability with    {}

  given ExceptCapability with    {}
  given IntersectCapability with {}

  given InsertReturningCapability with {}

  lazy val sqlRenderer: MariaDbAstRenderer[Codec] =
    new MariaDbAstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)
}
