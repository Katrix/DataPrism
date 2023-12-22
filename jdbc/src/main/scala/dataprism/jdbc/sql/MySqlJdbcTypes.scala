package dataprism.jdbc.sql

trait MySqlJdbcTypes extends JdbcAnsiTypes:
  val text: JdbcType[String] = JdbcType.simple("TEXT", _.getString(_), _.setString(_, _))

object MySqlJdbcTypes extends MySqlJdbcTypes:
  override def ArrayMapping: ArrayMappingCompanion =
    new ArrayMappingCompanion {}
