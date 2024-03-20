package dataprism.platform.implementations

import dataprism.sharedast.MySql8AstRenderer

trait MySql8QueryPlatform extends MySqlQueryPlatform {
  given LateralJoinCapability with {}

  given IntersectAllCapability with {}
  given ExceptAllCapability with    {}

  given ExceptCapability with    {}
  given IntersectCapability with {}

  lazy val sqlRenderer: MySql8AstRenderer[Codec] =
    new MySql8AstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)
}
