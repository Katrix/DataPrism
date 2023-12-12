package dataprism.skunk.platform.implementations

import dataprism.platform.implementations.PostgresQueryPlatform
import dataprism.skunk.sql.SkunkAnsiTypes
import dataprism.sql.AnsiTypes
import skunk.Codec
import skunk.data.Arr

class PostgresSkunkPlatform extends PostgresQueryPlatform {

  override type ArrayTypeArgs[_] = DummyImplicit
  override type Type[A]          = Codec[A]

  override def arrayType[A](elemType: Codec[A])(using extraArrayTypeArgs: DummyImplicit): Codec[Seq[A]] =
    Codec
      .array[A](
        a => elemType.encode(a).head.get,
        s => elemType.decode(0, List(Some(s))).left.map(_.message),
        elemType.types.head
      )
      .imap(arr => Seq.tabulate(arr.size)(arr.get(_).get))(seq => Arr(seq: _*))

  override def ansiTypes: AnsiTypes[Codec] = SkunkAnsiTypes
}
