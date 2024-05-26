package dataprism.platform.sql.implementations

import dataprism.sharedast.MySql57AstRenderer

trait MySql57Platform extends MySqlPlatform {
  lazy val sqlRenderer: MySql57AstRenderer[Codec] =
    new MySql57AstRenderer[Codec](AnsiTypes, [A] => (codec: Codec[A]) => codec.name)
}
