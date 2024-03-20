package dataprism.sharedast

import dataprism.sql.AnsiTypes

class MySql8AstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec], getCodecTypeName: [A] => Codec[A] => String)
  extends MySqlAstRenderer[Codec](ansiTypes, getCodecTypeName)
