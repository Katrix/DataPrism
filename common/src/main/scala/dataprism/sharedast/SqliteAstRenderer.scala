package dataprism.sharedast

import dataprism.sql.*

//noinspection SqlNoDataSourceInspection
class SqliteAstRenderer[Codec[_]](ansiTypes: AnsiTypes[Codec]) extends AstRenderer[Codec](ansiTypes) {

}
