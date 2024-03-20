package dataprism.platform.implementations

import dataprism.sharedast.MySql57AstRenderer

trait MySql57QueryPlatform extends MySqlQueryPlatform {
  lazy val sqlRenderer: MySql57AstRenderer[Codec] =
    new MySql57AstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)
}
