package dataprism.platform.sql.implementations

import dataprism.sharedast.MySql8AstRenderer

trait MySql8Platform extends MySqlPlatform {
  given LateralJoinCapability with {}

  given IntersectAllCapability with {}
  given ExceptAllCapability with    {}

  given ExceptCapability with    {}
  given IntersectCapability with {}

  lazy val sqlRenderer: MySql8AstRenderer[Codec] =
    new MySql8AstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)
}
