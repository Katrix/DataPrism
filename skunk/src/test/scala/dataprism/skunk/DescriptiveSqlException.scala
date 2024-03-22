package dataprism.skunk

class DescriptiveSqlException(sql: String, cause: Throwable)
    extends Exception(Option(cause.getMessage).getOrElse("") + s"\n\nSql statement: $sql", cause)
